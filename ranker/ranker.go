//Package ranker 排序器类型
//ranker可以用于分布式排序操作
package ranker

import (
	"context"
	"time"

	helper "github.com/Golang-Tools/redishelper"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/common/log"
)

//Ranker 排序工具
type Ranker struct {
	Key    string
	MaxTTL time.Duration
	client helper.GoRedisV8Client
}

//New 新建一个排序器
func New(client helper.GoRedisV8Client, key string, maxttl ...time.Duration) *Ranker {
	r := new(Ranker)
	r.client = client
	r.Key = key
	switch len(maxttl) {
	case 0:
		{
			return r
		}
	case 1:
		{
			if maxttl[0] != 0 {
				r.MaxTTL = maxttl[0]
				return r
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return r
		}
	default:
		{
			log.Warn("ttl最多只能设置一个,使用第一个作为过期时间")
			if maxttl[0] != 0 {
				r.MaxTTL = maxttl[0]
				return r
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return r
		}
	}
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
func (r *Ranker) RefreshTTL(ctx context.Context) error {
	if r.MaxTTL != 0 {
		_, err := r.client.Exists(ctx, r.Key).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
			return ErrKeyNotExist
		}
		_, err = r.client.Expire(ctx, r.Key, r.MaxTTL).Result()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrBitmapNotSetMaxTLL
}

//TTL 查看key的剩余时间
func (r *Ranker) TTL(ctx context.Context) (time.Duration, error) {
	_, err := r.client.Exists(ctx, r.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
		return 0, ErrKeyNotExist
	}
	res, err := r.client.TTL(ctx, r.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

// 写操作

//Push 增加若干个新元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要添加的带权重的元素
func (r *Ranker) Push(ctx context.Context, refreshTTL bool, elements ...*redis.Z) (int64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZAddNX(ctx, r.Key, elements...).Result()
}

//Update 更新元素的权重
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要更新的带权重的元素
func (r *Ranker) Update(ctx context.Context, refreshTTL bool, elements ...*redis.Z) (int64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZAddXX(ctx, r.Key, elements...).Result()
}

//PushOrUpdate 如果元素存在则更新元素权重,不存在则增加元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要添加或更新的带权重的元素
func (r *Ranker) PushOrUpdate(ctx context.Context, refreshTTL bool, elements ...*redis.Z) (int64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZAdd(ctx, r.Key, elements...).Result()
}

//Reset 重置排名器
func (r *Ranker) Reset(ctx context.Context) error {
	_, err := r.client.Del(ctx, r.Key).Result()
	if err != nil {
		return err
	}
	return nil
}

//IncrWeight 为元素累增1的权重,
func (r *Ranker) IncrWeight(ctx context.Context, refreshTTL bool, elementkey string) (float64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZIncrBy(ctx, r.Key, float64(1), elementkey).Result()
}

//IncrWeightM 为元素累增一定数值的权重
func (r *Ranker) IncrWeightM(ctx context.Context, refreshTTL bool, elementkey string, weight float64) (float64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZIncrBy(ctx, r.Key, weight, elementkey).Result()
}

// Remove 删除元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...interface{} 要删除的元素
func (r *Ranker) Remove(ctx context.Context, refreshTTL bool, elements ...interface{}) (int64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	return r.client.ZRem(ctx, r.Key, elements...).Result()
}

//读操作

// Len 获取排名器的当前长度
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (r *Ranker) Len(ctx context.Context) (int64, error) {
	return r.client.ZCard(ctx, r.Key).Result()
}

//Range 获取排名范围内的元素,reverse为True则为从大到小否则为从小到大
func (r *Ranker) Range(ctx context.Context, refreshTTL bool, reverse bool, scop ...int64) ([]string, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	lenScop := len(scop)
	switch lenScop {
	case 0:
		{
			if reverse {
				return r.client.ZRevRange(ctx, r.Key, int64(0), int64(-1)).Result()
			}
			return r.client.ZRange(ctx, r.Key, int64(0), int64(-1)).Result()
		}
	case 1:
		{
			if reverse {
				return r.client.ZRevRange(ctx, r.Key, scop[0], int64(-1)).Result()
			}
			return r.client.ZRange(ctx, r.Key, scop[0], int64(-1)).Result()
		}
	case 2:
		{
			if reverse {
				return r.client.ZRevRange(ctx, r.Key, scop[0], scop[1]).Result()
			}
			return r.client.ZRange(ctx, r.Key, scop[0], scop[1]).Result()
		}
	default:
		{
			return nil, ErrIndefiniteParameterLength
		}
	}

}

//First 获取排名前若干位的元素,reverse为True则为从大到小否则为从小到大
func (r *Ranker) First(ctx context.Context, refreshTTL bool, count int64, reverse bool) ([]string, error) {
	if count <= 0 {
		return nil, ErrCountMustBePositive
	}
	return r.Range(ctx, refreshTTL, reverse, int64(0), count-1)
}

//Head first的别名
func (r *Ranker) Head(ctx context.Context, refreshTTL bool, count int64, reverse bool) ([]string, error) {
	return r.First(ctx, refreshTTL, count, reverse)
}

//Last 获取排名后若干位的元素,reverse为True则为从大到小否则为从小到大
func (r *Ranker) Last(ctx context.Context, refreshTTL bool, count int64, reverse bool) ([]string, error) {
	if count <= 0 {
		return nil, ErrCountMustBePositive
	}
	return r.Range(ctx, refreshTTL, !reverse, 0, count-1)
}

//Tail Last的别名
func (r *Ranker) Tail(ctx context.Context, refreshTTL bool, count int64, reverse bool) ([]string, error) {
	return r.Last(ctx, refreshTTL, count, reverse)
}

//GetRank 获取指定元素的排名,reverse为True则为从大到小否则为从小到大
func (r *Ranker) GetRank(ctx context.Context, refreshTTL bool, element string, reverse bool) (int64, error) {
	if refreshTTL {
		defer r.RefreshTTL(ctx)
	}
	if reverse {
		res, err := r.client.ZRevRank(ctx, r.Key, element).Result()
		if err != nil {
			if err == redis.Nil {
				return -1, ErrElementNotExist
			}
			return -1, err
		}
		return res + 1, nil
	}
	res, err := r.client.ZRank(ctx, r.Key, element).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, ErrElementNotExist
		}
		return -1, err
	}
	return res + 1, nil

}

//GetElementByRank 获取指定排名的元素,reverse为True则为从大到小否则为从小到大
func (r *Ranker) GetElementByRank(ctx context.Context, refreshTTL bool, rank int64, reverse bool) (string, error) {
	res, err := r.Range(ctx, refreshTTL, reverse, rank, rank)
	if err != nil {
		return "", err
	}
	if len(res) != 1 {
		return "", ErrRankerror
	}
	return res[0], nil
}
