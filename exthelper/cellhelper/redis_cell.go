//cellhelper redis扩展[redis-cell](https://github.com/brandur/redis-cell)帮助模块,用于返回令牌桶状态
package cellhelper

import (
	"context"
	"errors"
	"fmt"
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

//ClThrottle 增加桶中token数并返回桶状态
//@params count int64 增加的token数量
//@params  opts ...optparams.Option[ClThrottleOpts] 其他参数
func (c *RedisCell) ClThrottle(ctx context.Context, count int64, opts ...optparams.Option[ClThrottleOpts]) (*RedisCellStatus, error) {
	defaultopt := ClThrottleOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 {
		ttl = refreshopt.TTL
	} else {
		if c.MaxTTL() > 0 {
			ttl = c.MaxTTL()
		}
	}
	maxBurst := c.opt.MaxBurst
	if defaultopt.MaxBurst > 0 {
		maxBurst = defaultopt.MaxBurst
	}
	countPerPeriod := c.opt.CountPerPeriod
	if defaultopt.CountPerPeriod > 0 {
		countPerPeriod = defaultopt.CountPerPeriod
	}
	period := c.opt.Period
	if defaultopt.Period > 0 {
		period = defaultopt.Period
	}
	cmd := "CL.THROTTLE"
	// cmd := []interface{}{"CL.THROTTLE", c.Key(), maxBurst, countPerPeriod, period, count}
	var infos []interface{}
	var ok bool
	if ttl > 0 && refreshopt.RefreshTTL != middlewarehelper.RefreshTTLType_NoRefresh {
		if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
			res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, maxBurst, countPerPeriod, period, count)
			if err != nil {
				return nil, err
			}
			infos, ok = res.([]interface{})
		} else {
			exists, err := c.MiddleWareAbc.Exists(ctx)
			if err != nil {
				return nil, err
			}
			var res interface{}
			if exists {
				res, err = c.Client().Do(ctx, cmd, c.Key(), maxBurst, countPerPeriod, period, count).Result()
			} else {
				res, err = c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, maxBurst, countPerPeriod, period, count)
			}
			if err != nil {
				return nil, err
			}
			infos, ok = res.([]interface{})
		}
	} else {
		res, err := c.Client().Do(ctx, cmd, c.Key(), maxBurst, countPerPeriod, period, count).Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	}
	if !ok {
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", cmd)
	}
	if len(infos) != 5 {
		return nil, fmt.Errorf("%s results not ok", cmd)
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
