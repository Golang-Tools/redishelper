//Package counter Counter操作支持
//Counter可以用于分布式累加计数
package counter

import (
	"context"

	"github.com/Golang-Tools/redishelper/clientkey"
)

//Counter 分布式计数器
type Counter struct {
	*clientkey.ClientKey
}

//New 创建一个新的位图对象
//@params k *key.Key redis客户端的键对象
func New(k *clientkey.ClientKey) *Counter {
	bm := new(Counter)
	bm.ClientKey = k
	return bm
}

//NextM 加m后的当前计数
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params value int64 要增加的值.这个值可以为负
func (c *Counter) NextM(ctx context.Context, value int64) (int64, error) {
	if c.Opt.MaxTTL != 0 {
		defer c.RefreshTTL(ctx)
	}
	res, err := c.Client.IncrBy(ctx, c.Key, value).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//Next 加1后的当前计数值
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Next(ctx context.Context) (int64, error) {
	return c.NextM(ctx, 1)
}

//Len 当前的计数量
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Len(ctx context.Context) (int64, error) {
	return c.NextM(ctx, 0)
}

//Reset 重置当前计数器
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Reset(ctx context.Context) error {
	err := c.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}
