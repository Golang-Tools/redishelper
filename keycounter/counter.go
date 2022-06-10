//Package keycounter string型计数器
package keycounter

import (
	"context"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//Counter 分布式计数器
type Counter struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的位图对象
//@params cli redis.UniversalClient redis客户端对象
//@params opts ...optparams.Option[Options] 配置项
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Counter, error) {
	bm := new(Counter)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	m, err := middlewarehelper.New(cli, "counter", bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = m
	return bm, nil
}

//NextN 加m后的当前计数
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params value int64 要增加的值.这个值可以为负
func (c *Counter) NextN(ctx context.Context, value int64) (int64, error) {
	defer func() {
		err := c.RefreshTTL(ctx)
		if err != nil {
			c.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
		}
	}()

	res, err := c.Client().IncrBy(ctx, c.Key(), value).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//Next 加1后的当前计数值
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Next(ctx context.Context) (int64, error) {
	return c.NextN(ctx, 1)
}

//Len 当前的计数量
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Len(ctx context.Context) (int64, error) {
	return c.NextN(ctx, 0)
}

//Reset 重置当前计数器
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *Counter) Reset(ctx context.Context) error {
	return c.Delete(ctx)
}
