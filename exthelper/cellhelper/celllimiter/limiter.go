//Package celllimiter 限制器
//可以用于防止短时间内大量请求同时处理,比如缓存防击穿,防爬虫等
package celllimiter

import (
	"context"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/cellhelper"
	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	"github.com/go-redis/redis/v8"
)

//Limiter 分布式限制器
type Limiter struct {
	opt Options
	*cellhelper.RedisCell
	*limiterhelper.LimiterABC
}

//New 构造令牌桶限流器
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Limiter, error) {
	k := new(Limiter)
	k.opt = defaultOptions
	optparams.GetOption(&k.opt, opts...)
	l, err := limiterhelper.New(k.opt.LimiterOpts...)
	if err != nil {
		return nil, err
	}
	k.opt.CellOpts = append(k.opt.CellOpts, cellhelper.WithMiddlewaretype("limiter"))
	c, err := cellhelper.New(cli, k.opt.CellOpts...)
	if err != nil {
		return nil, err
	}
	k.LimiterABC = l
	k.RedisCell = c
	return k, nil
}

func (c *Limiter) Flood(ctx context.Context, value int64) (bool, error) {
	res, err := c.ClThrottle(ctx, value, cellhelper.RefreshTTL())
	if err != nil {
		return true, err
	}
	size := (res.Max - res.Remaining)

	full := c.LimiterABC.CheckWaterline(size, res.Blocked)
	return !full, nil
}

func (c *Limiter) WaterLevel(ctx context.Context) (int64, error) {
	res, err := c.ClThrottle(ctx, 0, cellhelper.RefreshTTL())
	if err != nil {
		return 0, err
	}
	return (res.Max - res.Remaining), err
}

//IsFull 观测水位是否已满
func (c *Limiter) IsFull(ctx context.Context) (bool, error) {
	notfull, err := c.Flood(ctx, 0)
	return !notfull, err
}
func (c *Limiter) Reset(ctx context.Context) error {
	return c.Clean(ctx)
}
