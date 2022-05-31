//Package limiter 限制器
//可以用于防止短时间内大量请求同时处理,比如缓存防击穿,防爬虫等
package limiter

import (
	"context"

	"github.com/Golang-Tools/redishelper/v2/ext/redis_cell"
)

//Limiter 分布式限制器
type Limiter struct {
	cell *redis_cell.RedisCell
	opt  Options
}

//FromRedisCell 从令牌桶对象中转化得到限制器
func New(cell *redis_cell.RedisCell, opts ...Option) (*Limiter, error) {
	c := new(Limiter)
	c.cell = cell
	c.opt = Defaultopt
	for _, opt := range opts {
		opt.Apply(&c.opt)
	}
	if c.Capacity() <= c.opt.WarningSize {
		return nil, ErrLimiterMaxSizeMustLargerThanWaringSize
	}
	return c, nil
}

func (c *Limiter) Flood(ctx context.Context, value int64) (bool, error) {
	res, err := c.cell.ClThrottle(ctx, value)
	if err != nil {
		return true, err
	}
	if res.Blocked {
		for _, hook := range c.opt.FullHooks {
			if c.opt.AsyncHooks {
				go hook(res)
			} else {
				hook(res)
			}
		}
	} else {
		if (res.Max - res.Remaining) >= c.opt.WarningSize {
			for _, hook := range c.opt.WarningHooks {
				if c.opt.AsyncHooks {
					go hook(res)
				} else {
					hook(res)
				}
			}
		}
	}
	return res.Blocked, nil
}

func (c *Limiter) WaterLevel(ctx context.Context) (int64, error) {
	res, err := c.cell.ClThrottle(ctx, 0)
	if err != nil {
		return 0, err
	}
	return (res.Max - res.Remaining), err
}

//Capacity 限制器最大容量
func (c *Limiter) Capacity() int64 {
	return c.cell.Capacity()
}

//IsFull 观测水位是否已满
func (c *Limiter) IsFull(ctx context.Context) (bool, error) {
	notfull, err := c.Flood(ctx, 0)
	return !notfull, err
}
func (c *Limiter) Reset(ctx context.Context) error {
	_, err := c.cell.Client.Del(ctx, c.cell.Key).Result()
	if err != nil {
		return err
	}
	return nil
}
