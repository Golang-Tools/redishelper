//cellhelper redis扩展[redis-cell](https://github.com/brandur/redis-cell)帮助模块,用于返回令牌桶状态
package cellhelper

import (
	"context"
	"errors"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

type ClThrottleInPipePlacehold struct {
	Cmd *redis.Cmd
}

func (r *ClThrottleInPipePlacehold) Result() (*RedisCellStatus, error) {
	res := r.Cmd.Val()
	infos, ok := res.([]interface{})
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

//ClThrottleInPipe 增加桶中token数并返回桶状态
//@params count int64 增加的token数量
//@params  opts ...optparams.Option[ClThrottleOpts] 其他参数,注意默认MaxBurst: 99,CountPerPeriod: 30,Period: 60
func ClThrottleInPipe(pipe redis.Pipeliner, ctx context.Context, key string, count int64, opts ...optparams.Option[ClThrottleOpts]) *ClThrottleInPipePlacehold {
	defaultopt := ClThrottleOpts{
		MaxBurst:       99,
		CountPerPeriod: 30,
		Period:         60,
	}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	maxBurst := int64(0)
	if defaultopt.MaxBurst > 0 {
		maxBurst = defaultopt.MaxBurst
	}
	countPerPeriod := int64(0)
	if defaultopt.CountPerPeriod > 0 {
		countPerPeriod = defaultopt.CountPerPeriod
	}
	period := int64(0)
	if defaultopt.Period > 0 {
		period = defaultopt.Period
	}
	res := ClThrottleInPipePlacehold{}
	cmd := "CL.THROTTLE"
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, maxBurst, countPerPeriod, period, count)

	} else {
		res.Cmd = pipe.Do(ctx, cmd, key, maxBurst, countPerPeriod, period, count)
	}
	return &res
}
