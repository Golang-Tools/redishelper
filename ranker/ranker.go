//Package ranker 排序器类型
//ranker可以用于分布式排序操作
//即多个worker分别计算更新一部分元素的权重,业务端则只负责读取结果
package ranker

import (
	"context"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//Ranker 排序工具
type Ranker struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 新建一个排序器
//@params k *key.Key redis客户端的键对象
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Ranker, error) {
	bm := new(Ranker)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	m, err := middlewarehelper.New(cli, "counter", bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = m
	return bm, nil
}

// 写操作

//AddM 增加若干个新元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...*redis.Z 要添加的带权重的元素
func (r *Ranker) AddM(ctx context.Context, elements ...*redis.Z) error {
	if r.MaxTTL() != 0 {
		_, err := r.Client().TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAddNX(ctx, r.Key(), elements...)
			pipe.Expire(ctx, r.Key(), r.MaxTTL())
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client().ZAddNX(ctx, r.Key(), elements...).Result()
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
	if r.MaxTTL() != 0 {
		_, err := r.Client().TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAddXX(ctx, r.Key(), elements...)
			pipe.Expire(ctx, r.Key(), r.MaxTTL())
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client().ZAddXX(ctx, r.Key(), elements...).Result()
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
	if r.MaxTTL() != 0 {
		_, err := r.Client().TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZAdd(ctx, r.Key(), elements...)
			pipe.Expire(ctx, r.Key(), r.MaxTTL())
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client().ZAdd(ctx, r.Key(), elements...).Result()
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
	return r.Delete(ctx)
}

//IncrWeightM 为元素累增一定数值的权重
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elementkey string 元素的key
//@params weight float64 元素的权重
func (r *Ranker) IncrWeightM(ctx context.Context, elementkey string, weight float64) (float64, error) {
	defer func() {
		err := r.RefreshTTL(ctx)
		if err != nil {
			r.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
		}
	}()
	return r.Client().ZIncrBy(ctx, r.Key(), weight, elementkey).Result()
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
	defer func() {
		err := r.RefreshTTL(ctx)
		if err != nil {
			r.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
		}
	}()
	return r.Client().ZScore(ctx, r.Key(), elementkey).Result()
}

//RemoveM 删除元素
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params elements ...interface{} 要删除的元素
func (r *Ranker) RemoveM(ctx context.Context, elements ...interface{}) error {
	if r.MaxTTL() != 0 {
		_, err := r.Client().TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, r.Key(), elements...)
			pipe.Expire(ctx, r.Key(), r.MaxTTL())
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := r.Client().ZRem(ctx, r.Key(), elements...).Result()
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
	return r.Client().ZCard(ctx, r.Key()).Result()
}

type Rangescopopts struct {
	Reverse bool
	From    int64
	To      int64
}

var defaultrangescopopts = Rangescopopts{
	From: 0,
	To:   -1,
}

//ScopFrom 排序的下限
func ScopFrom(from int64) optparams.Option[Rangescopopts] {
	return optparams.NewFuncOption(func(o *Rangescopopts) {
		o.From = from
	})
}

//ScopTo 排序的上限
func ScopTo(to int64) optparams.Option[Rangescopopts] {
	return optparams.NewFuncOption(func(o *Rangescopopts) {
		o.To = to
	})
}

//Reverse 控制排序从是否从大到小排序
func Reverse() optparams.Option[Rangescopopts] {
	return optparams.NewFuncOption(func(o *Rangescopopts) {
		o.Reverse = true
	})
}

//Range 获取排名范围内的元素,默认全量从小到大排序.
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params opts ...optparams.Option[Rangescopopts]
func (r *Ranker) Range(ctx context.Context, opts ...optparams.Option[Rangescopopts]) ([]string, error) {
	defer func() {
		err := r.RefreshTTL(ctx)
		if err != nil {
			r.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
		}
	}()
	opt := defaultrangescopopts
	optparams.GetOption(&opt, opts...)
	if opt.Reverse {
		return r.Client().ZRevRange(ctx, r.Key(), opt.From, opt.To).Result()
	}
	return r.Client().ZRange(ctx, r.Key(), opt.From, opt.To).Result()
}

//First 获取排名前若干位的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 前几位
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) First(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error) {
	if n <= 0 {
		return nil, ErrParamNMustBePositive
	}
	opt := defaultrangescopopts
	optparams.GetOption(&opt, opts...)
	_opts := []optparams.Option[Rangescopopts]{ScopFrom(0), ScopTo(n - 1)}
	if opt.Reverse {
		_opts = append(_opts, Reverse())
	}
	return r.Range(ctx, _opts...)
}

//Head first的别名
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 前几位
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) Head(ctx context.Context, count int64, opts ...optparams.Option[Rangescopopts]) ([]string, error) {
	return r.First(ctx, count, opts...)
}

//Last 获取排名后若干位的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 后几位
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) Last(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error) {
	if n <= 0 {
		return nil, ErrParamNMustBePositive
	}
	opt := defaultrangescopopts
	optparams.GetOption(&opt, opts...)
	_opts := []optparams.Option[Rangescopopts]{ScopFrom(0), ScopTo(n - 1)}
	if !opt.Reverse {
		_opts = append(_opts, Reverse())
	}

	return r.Range(ctx, _opts...)
}

//Tail Last的别名
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params n int64 后几位
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) Tail(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error) {
	return r.Last(ctx, n, opts...)
}

//GetRank 获取指定元素的排名,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 前几位
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) GetRank(ctx context.Context, element string, opts ...optparams.Option[Rangescopopts]) (int64, error) {
	defer func() {
		err := r.RefreshTTL(ctx)
		if err != nil {
			r.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
		}
	}()
	opt := defaultrangescopopts
	optparams.GetOption(&opt, opts...)
	if opt.Reverse {
		res, err := r.Client().ZRevRank(ctx, r.Key(), element).Result()
		if err != nil {
			if err == redis.Nil {
				return -1, ErrElementNotExist
			}
			return -1, err
		}
		return res + 1, nil
	}
	res, err := r.Client().ZRank(ctx, r.Key(), element).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, ErrElementNotExist
		}
		return -1, err
	}
	return res + 1, nil

}

//GetElementByRank 获取指定排名的元素,reverse为True则为从大到小否则为从小到大
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params rank int64 第几名
//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
func (r *Ranker) GetElementByRank(ctx context.Context, rank int64, opts ...optparams.Option[Rangescopopts]) (string, error) {
	opt := defaultrangescopopts
	optparams.GetOption(&opt, opts...)
	_opts := []optparams.Option[Rangescopopts]{ScopFrom(rank), ScopTo(rank)}
	if opt.Reverse {
		_opts = append(_opts, Reverse())
	}

	res, err := r.Range(ctx, _opts...)
	if err != nil {
		return "", err
	}
	if len(res) != 1 {
		return "", ErrRankerror
	}
	return res[0], nil
}
