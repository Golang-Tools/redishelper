//cellhelper redis扩展[redis-cell](https://github.com/brandur/redis-cell)帮助模块,用于返回令牌桶状态
package cellhelper

import (
	"context"
	"errors"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//RedisCellStatus 令牌桶的状态信息
type RedisCellStatus struct {
	Blocked          bool  `json:"blocked"`
	Max              int64 `json:"max"`
	Remaining        int64 `json:"remaining"`
	WaitForRetryTime int64 `json:"wait_for_retry_time"` //单位s,为-1表示不用等
	ResetToMaxTime   int64 `json:"reset_to_max_time"`   //单位s
}

//RedisCell 令牌桶对象
type RedisCell struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的令牌桶对象
//@params cli redis.UniversalClient
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*RedisCell, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := exthelper.CheckModule(cli, ctx, "redis-cell")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module redis-cell yet")
	}
	bm := new(RedisCell)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	mi, err := middlewarehelper.New(cli, bm.opt.Middlewaretype, bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = mi
	return bm, nil
}

// //Capacity 最大容量
// func (c *RedisCell) Capacity() int64 {
// 	return c.opt.MaxBurst + 1
// }

//ClThrottle 增加桶中token数并返回桶状态
//@params count int64 增加的token数量
func (c *RedisCell) ClThrottle(ctx context.Context, count int64) (*RedisCellStatus, error) {
	var infos []interface{}
	var ok bool
	if c.MaxTTL() > 0 {
		pipe := c.Client().TxPipeline()
		res_cmd := pipe.Do(ctx, "CL.THROTTLE", c.Key(), c.opt.MaxBurst, c.opt.CountPerPeriod, c.opt.Period, count)
		pipe.Expire(ctx, c.Key(), c.MaxTTL())
		_, err := pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}
		res, err := res_cmd.Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	} else {
		res, err := c.Client().Do(ctx, "CL.THROTTLE", c.Key(), c.opt.MaxBurst, c.opt.CountPerPeriod, c.opt.Period, count).Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	}
	if !ok {
		return nil, errors.New("cannot parser ClThrottle results to []interface{}")
	}
	if len(infos) != 5 {
		return nil, errors.New("ClThrottle results not ok")
	}
	var block bool
	if infos[0].(int64) == int64(0) {
		block = false
	} else {
		block = true
	}
	result := &RedisCellStatus{
		Blocked:          block,
		Max:              infos[1].(int64),
		Remaining:        infos[2].(int64),
		WaitForRetryTime: infos[3].(int64),
		ResetToMaxTime:   infos[4].(int64),
	}
	return result, nil
}

//Clean 清除cell的key
func (c *RedisCell) Clean(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}
