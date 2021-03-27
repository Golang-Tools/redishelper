//Package limiter 限制器
//可以用于防止短时间内大量请求同时处理,比如缓存防击穿,防爬虫等
package limiter

import (
	"context"

	"github.com/go-redis/redis/v8"
)

//Limiter 分布式限制器
type Limiter struct {
	Key    string
	opt    Options
	Client redis.UniversalClient
}

//New 创建一个限制器
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params opts ...Option limiter的可设置项
func New(client redis.UniversalClient, key string, opts ...Option) (*Limiter, error) {
	k := new(Limiter)
	k.Client = client
	k.Key = key
	k.opt = Defaultopt
	for _, opt := range opts {
		opt.Apply(&k.opt)
	}
	if k.opt.MaxSize <= k.opt.WarningSize {
		return nil, ErrLimiterMaxSizeMustLargerThanWaringSize
	}
	return k, nil
}

func (c *Limiter) Flood(ctx context.Context, value int64) (bool, error) {
	// if c.opt.MaxTTL != 0 {
	// 	defer c.RefreshTTL(ctx)
	// }
	first := false
	flag, err := c.Client.Exists(ctx, c.Key).Result()
	if err != nil {
		return true, err
	}
	if flag != 0 {
		first = true
	}
	res, err := c.Client.IncrBy(ctx, c.Key, value).Result()
	if err != nil {
		return true, err
	}
	if first && c.opt.MaxTTL != 0 {
		_, err := c.Client.Expire(ctx, c.Key, c.opt.MaxTTL).Result()
		if err != nil {
			return true, err
		}
	}
	if res >= c.opt.MaxSize {
		for _, hook := range c.opt.FullHooks {
			if c.opt.AsyncHooks {
				go hook(res, c.opt.MaxSize)
			} else {
				hook(res, c.opt.MaxSize)
			}
		}
		// 删除添加的值避免超限
		if c.opt.MaxSize-res < 0 {
			go c.Client.IncrBy(ctx, c.Key, c.opt.MaxSize-res).Result()
		}
		return false, nil
	}
	if res >= c.opt.WarningSize {
		for _, hook := range c.opt.WarningHooks {
			if c.opt.AsyncHooks {
				go hook(res, c.opt.MaxSize)
			} else {
				hook(res, c.opt.MaxSize)
			}
		}
	}
	return true, nil
}

func (c *Limiter) WaterLevel(ctx context.Context) (int64, error) {
	res, err := c.Client.IncrBy(ctx, c.Key, 0).Result()
	if err != nil {
		return 0, err
	}
	return res, err
}

//Capacity 限制器最大容量
func (c *Limiter) Capacity() int64 {
	return c.opt.MaxSize
}

//IsFull 观测水位是否已满
func (c *Limiter) IsFull(ctx context.Context) (bool, error) {
	notfull, err := c.Flood(ctx, 0)
	return !notfull, err
}
func (c *Limiter) Reset(ctx context.Context) error {
	_, err := c.Client.Del(ctx, c.Key).Result()
	if err != nil {
		return err
	}
	return nil
}
