//Package ranker 排序器类型
//ranker可以用于分布式排序操作
//即多个worker分别计算更新一部分元素的权重,业务端则只负责读取结果
package ranker

import (
	"context"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/exception"
	"github.com/go-redis/redis/v8"
)

//Ranker 排序工具
type Ranker struct {
	*clientkey.ClientKey
}

//New 新建一个排序器
//@params k *key.Key redis客户端的键对象
func New(k *clientkey.ClientKey) *Ranker {
	bm := new(Ranker)
	bm.ClientKey = k
	return bm
}

// 写操作

//AddM 增加若干个新元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要添加的带权重的元素
func (r *Ranker) AddM(ctx context.Context, elements ...*redis.Z) error {
	if r.Opt.MaxTTL != 0 {
		_, err := r.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAddNX(ctx, r.Key, elements...)
			pipe.Expire(ctx, r.Key, r.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client.ZAddNX(ctx, r.Key, elements...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Add 增加一个新元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements *redis.Z 要添加的带权重的元素
func (r *Ranker) Add(ctx context.Context, element *redis.Z) error {
	return r.AddM(ctx, element)
}

//UpdateM 更新元素的权重
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要更新的带权重的元素
func (r *Ranker) UpdateM(ctx context.Context, elements ...*redis.Z) error {
	if r.Opt.MaxTTL != 0 {
		_, err := r.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAddXX(ctx, r.Key, elements...)
			pipe.Expire(ctx, r.Key, r.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client.ZAddXX(ctx, r.Key, elements...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Update 更新一个元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements *redis.Z 要更新的带权重的元素
func (r *Ranker) Update(ctx context.Context, element *redis.Z) error {
	return r.UpdateM(ctx, element)
}

//AddOrUpdateM 如果元素存在则更新元素权重,不存在则增加元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要添加或更新的带权重的元素
func (r *Ranker) AddOrUpdateM(ctx context.Context, elements ...*redis.Z) error {
	if r.Opt.MaxTTL != 0 {
		_, err := r.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAdd(ctx, r.Key, elements...)
			pipe.Expire(ctx, r.Key, r.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client.ZAdd(ctx, r.Key, elements...).Result()
	if err != nil {
		return err
	}
	return nil
}

//AddOrUpdate 如果元素存在则更新元素权重,不存在则增加元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params element *redis.Z 要添加或更新的带权重的元素
func (r *Ranker) AddOrUpdate(ctx context.Context, element *redis.Z) error {
	return r.AddOrUpdateM(ctx, element)
}

//Reset 重置排名器
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (r *Ranker) Reset(ctx context.Context) error {
	err := r.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

//IncrWeightM 为元素累增一定数值的权重
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elementkey string 元素的key
//@params weight float64 元素的权重
func (r *Ranker) IncrWeightM(ctx context.Context, elementkey string, weight float64) (float64, error) {
	if r.Opt.MaxTTL != 0 {
		defer r.RefreshTTL(ctx)
	}
	return r.Client.ZIncrBy(ctx, r.Key, weight, elementkey).Result()
}

//IncrWeight 为元素累增1的权重,
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elementkey string 元素的key
func (r *Ranker) IncrWeight(ctx context.Context, elementkey string) (float64, error) {
	return r.IncrWeightM(ctx, elementkey, float64(1))
}

//GetWeight 查看元素的权重
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elementkey string 元素的key
func (r *Ranker) GetWeight(ctx context.Context, elementkey string) (float64, error) {
	if r.Opt.MaxTTL != 0 {
		defer r.RefreshTTL(ctx)
	}
	return r.Client.ZScore(ctx, r.Key, elementkey).Result()
}

//RemoveM 删除元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...interface{} 要删除的元素
func (r *Ranker) RemoveM(ctx context.Context, elements ...interface{}) error {
	if r.Opt.MaxTTL != 0 {
		_, err := r.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, r.Key, elements...)
			pipe.Expire(ctx, r.Key, r.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client.ZRem(ctx, r.Key, elements...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Remove 删除元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params element interface{} 要删除的元素
func (r *Ranker) Remove(ctx context.Context, element interface{}) error {
	return r.RemoveM(ctx, element)
}

//读操作

// Len 获取排名器的当前长度
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (r *Ranker) Len(ctx context.Context) (int64, error) {
	return r.Client.ZCard(ctx, r.Key).Result()
}

//Range 获取排名范围内的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params reverse bool 倒序排序
//@params scop ...int64 排序范围
func (r *Ranker) Range(ctx context.Context, reverse bool, scop ...int64) ([]string, error) {
	if r.Opt.MaxTTL != 0 {
		defer r.RefreshTTL(ctx)
	}
	lenScop := len(scop)
	switch lenScop {
	case 0:
		{
			if reverse {
				return r.Client.ZRevRange(ctx, r.Key, int64(0), int64(-1)).Result()
			}
			return r.Client.ZRange(ctx, r.Key, int64(0), int64(-1)).Result()
		}
	case 1:
		{
			if reverse {
				return r.Client.ZRevRange(ctx, r.Key, scop[0], int64(-1)).Result()
			}
			return r.Client.ZRange(ctx, r.Key, scop[0], int64(-1)).Result()
		}
	case 2:
		{
			if reverse {
				return r.Client.ZRevRange(ctx, r.Key, scop[0], scop[1]).Result()
			}
			return r.Client.ZRange(ctx, r.Key, scop[0], scop[1]).Result()
		}
	default:
		{
			return nil, exception.ErrParamScopLengthMoreThan2
		}
	}

}

//First 获取排名前若干位的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 前几位
//@params reverse bool 倒序排序
func (r *Ranker) First(ctx context.Context, n int64, reverse bool) ([]string, error) {
	if n <= 0 {
		return nil, ErrParamNMustBePositive
	}
	return r.Range(ctx, reverse, int64(0), n-1)
}

//Head first的别名
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 前几位
//@params reverse bool 倒序排序
func (r *Ranker) Head(ctx context.Context, count int64, reverse bool) ([]string, error) {
	return r.First(ctx, count, reverse)
}

//Last 获取排名后若干位的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 后几位
//@params reverse bool 倒序排序
func (r *Ranker) Last(ctx context.Context, n int64, reverse bool) ([]string, error) {
	if n <= 0 {
		return nil, ErrParamNMustBePositive
	}
	return r.Range(ctx, !reverse, 0, n-1)
}

//Tail Last的别名
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 后几位
//@params reverse bool 倒序排序
func (r *Ranker) Tail(ctx context.Context, n int64, reverse bool) ([]string, error) {
	return r.Last(ctx, n, reverse)
}

//GetRank 获取指定元素的排名,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 前几位
//@params reverse bool 倒序排序
func (r *Ranker) GetRank(ctx context.Context, element string, reverse bool) (int64, error) {
	if r.Opt.MaxTTL != 0 {
		defer r.RefreshTTL(ctx)
	}
	if reverse {
		res, err := r.Client.ZRevRank(ctx, r.Key, element).Result()
		if err != nil {
			if err == redis.Nil {
				return -1, exception.ErrElementNotExist
			}
			return -1, err
		}
		return res + 1, nil
	}
	res, err := r.Client.ZRank(ctx, r.Key, element).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, exception.ErrElementNotExist
		}
		return -1, err
	}
	return res + 1, nil

}

//GetElementByRank 获取指定排名的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params rank int64 第几名
//@params reverse bool 倒序排序
func (r *Ranker) GetElementByRank(ctx context.Context, rank int64, reverse bool) (string, error) {
	res, err := r.Range(ctx, reverse, rank, rank)
	if err != nil {
		return "", err
	}
	if len(res) != 1 {
		return "", ErrRankerror
	}
	return res[0], nil
}
