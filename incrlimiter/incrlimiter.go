//Package limiter 限制器
//可以用于防止短时间内大量请求同时处理,比如缓存防击穿,防爬虫等
package incrlimiter

import (
	"context"
	"errors"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//Limiter 分布式限制器
type Limiter struct {
	opt Options
	*limiterhelper.LimiterABC
	*middlewarehelper.MiddleWareAbc
}

//New 创建一个限制器
//@params client redis.UniversalClient 客户端对象
//@params opts ...Option limiter的可设置项
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Limiter, error) {
	k := new(Limiter)
	k.opt = defaultOptions
	optparams.GetOption(&k.opt, opts...)
	l, err := limiterhelper.New(k.opt.LimiterOpts...)
	if err != nil {
		return nil, err
	}
	m, err := middlewarehelper.New(cli, "limiter", k.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	if m.MaxTTL() <= 0 {
		return nil, errors.New("incrlimiter must set MaxTTL")
	}
	k.MiddleWareAbc = m
	k.LimiterABC = l
	return k, nil
}

//Flood 灌注
//当返回为true说明注水成功,false表示无法注入(满了)
func (c *Limiter) Flood(ctx context.Context, value int64) (bool, error) {
	// if c.opt.MaxTTL != 0 {
	// 	defer c.RefreshTTL(ctx)
	// }
	first := false
	flag, err := c.Client().Exists(ctx, c.Key()).Result()
	if err != nil {
		return true, err
	}
	if flag != 0 {
		first = true
	}
	res, err := c.Client().IncrBy(ctx, c.Key(), value).Result()
	if err != nil {
		return true, err
	}
	if first && c.MaxTTL() != 0 {
		_, err := c.Client().Expire(ctx, c.Key(), c.MaxTTL()).Result()
		if err != nil {
			return true, err
		}
	}
	full := c.LimiterABC.CheckWaterline(res, false)
	if full {
		diff := c.Capacity() - res
		if diff < 0 {
			go c.Client().IncrBy(ctx, c.Key(), diff).Result()
		}
	}
	return !full, nil
}

//WaterLevel 当前水位
func (c *Limiter) WaterLevel(ctx context.Context) (int64, error) {
	res, err := c.Client().IncrBy(ctx, c.Key(), 0).Result()
	if err != nil {
		return 0, err
	}
	return res, err
}

//IsFull 观测水位是否已满
func (c *Limiter) IsFull(ctx context.Context) (bool, error) {
	notfull, err := c.Flood(ctx, 0)
	return !notfull, err
}
func (c *Limiter) Reset(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}
