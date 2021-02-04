//Package estimatecount 面向对象的计数估计类型
//estimatecount 用于粗略统计大量数据去重后的个数,一般用在日活计算
package estimatecount

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

//EstimateCount 估计计数对象
type EstimateCount struct {
	Key    string
	MaxTTL time.Duration
	client redis.UniversalClient
}

//New 新建一个估计计数对象
func New(client redis.UniversalClient, key string, maxttl time.Duration) *EstimateCount {
	counter := new(EstimateCount)
	counter.Key = key
	counter.MaxTTL = maxttl
	counter.client = client
	return counter
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (e *EstimateCount) RefreshTTL(ctx context.Context) error {
	if e.MaxTTL != 0 {
		_, err := e.client.Expire(ctx, e.Key, e.MaxTTL).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrEstimateCountNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (e *EstimateCount) TTL(ctx context.Context) (time.Duration, error) {
	_, err := e.client.Exists(ctx, e.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
		return 0, ErrKeyNotExist
	}
	res, err := e.client.TTL(ctx, e.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//写操作

//Add 添加数据
func (e *EstimateCount) Add(ctx context.Context, items ...interface{}) error {
	_, err := e.client.PFAdd(ctx, e.Key, items...).Result()
	if err != nil {
		return err
	}
	return nil
}
