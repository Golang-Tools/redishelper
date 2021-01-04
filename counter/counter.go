//Package counter 为代理添加Counter操作支持
//Counter可以用于分布式累加计数
package counter

import (
	"context"
	"time"

	helper "github.com/Golang-Tools/redishelper"
	"github.com/go-redis/redis"
	"github.com/prometheus/common/log"
)

//Counter 分布式计数器
type Counter struct {
	Key    string
	MaxTTL time.Duration
	client helper.GoRedisV8Client
}

//New 创建一个新的位图对象
func New(client helper.GoRedisV8Client, key string, maxttl ...time.Duration) *Counter {
	c := new(Counter)
	c.client = client
	c.Key = key
	switch len(maxttl) {
	case 0:
		{
			return c
		}
	case 1:
		{
			if maxttl[0] != 0 {
				c.MaxTTL = maxttl[0]
				return c
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return c
		}
	default:
		{
			log.Warn("ttl最多只能设置一个,使用第一个作为过期时间")
			if maxttl[0] != 0 {
				c.MaxTTL = maxttl[0]
				return c
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return c
		}
	}
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
func (c *Counter) RefreshTTL(ctx context.Context) error {
	if c.MaxTTL != 0 {
		_, err := c.client.Exists(ctx, c.Key).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
			return ErrKeyNotExist
		}
		_, err = c.client.Expire(ctx, c.Key, c.MaxTTL).Result()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrBitmapNotSetMaxTLL
}

//TTL 查看key的剩余时间
func (c *Counter) TTL(ctx context.Context) (time.Duration, error) {
	_, err := c.client.Exists(ctx, c.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
		return 0, ErrKeyNotExist
	}
	res, err := c.client.TTL(ctx, c.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//Next 加1后的当前计数值
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Next(ctx context.Context, refreshTTL bool) (int64, error) {
	if refreshTTL {
		defer c.RefreshTTL(ctx)
	}
	res, err := c.client.IncrBy(ctx, c.Key, 1).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//NextM 加m后的当前计数
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params value int64 要增加的值.这个值可以为负
func (c *Counter) NextM(ctx context.Context, refreshTTL bool, value int64) (int64, error) {
	if refreshTTL {
		defer c.RefreshTTL(ctx)
	}
	res, err := c.client.IncrBy(ctx, c.Key, value).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//Reset 重置当前计数器
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Reset(ctx context.Context) error {

	_, err := c.client.Del(ctx, c.Key).Result()
	if err != nil {
		return err
	}
	return nil
}
