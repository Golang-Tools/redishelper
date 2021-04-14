package redis_cell

import (
	"context"
	"errors"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/ext"
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
	*clientkey.ClientKey
	opt Options
}

//New 创建一个新的令牌桶对象
//@params k *key.Key redis客户端的键对象
func New(k *clientkey.ClientKey, opts ...Option) (*RedisCell, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := ext.CheckModule(k.Client, ctx, "redis-cell")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module redis-cell yet")
	}
	bm := new(RedisCell)
	bm.ClientKey = k
	bm.opt = Defaultopt
	for _, opt := range opts {
		opt.Apply(&bm.opt)
	}
	return bm, nil
}

//Capacity 最大容量
func (c *RedisCell) Capacity() int64 {
	return c.opt.MaxBurst + 1
}

//ClThrottle 增加桶中token数并返回桶状态
//@params count int64 增加的token数量
func (c *RedisCell) ClThrottle(ctx context.Context, count int64) (*RedisCellStatus, error) {
	res, err := c.Client.Do(ctx, "CL.THROTTLE", c.Key, c.opt.MaxBurst, c.opt.CountPerPeriod, c.opt.Period, count).Result()
	if err != nil {
		return nil, err
	}
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
