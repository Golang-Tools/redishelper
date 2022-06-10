package ranker

import (
	"context"

	"github.com/Golang-Tools/optparams"
	"github.com/go-redis/redis/v8"
)

type RankerInterface interface {
	//AddM 增加若干个新元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements ...*redis.Z 要添加的带权重的元素
	AddM(ctx context.Context, elements ...*redis.Z) error

	//Add 增加一个新元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements *redis.Z 要添加的带权重的元素
	Add(ctx context.Context, element *redis.Z) error
	//UpdateM 更新元素的权重
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements ...*redis.Z 要更新的带权重的元素
	UpdateM(ctx context.Context, elements ...*redis.Z) error

	//Update 更新一个元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements *redis.Z 要更新的带权重的元素
	Update(ctx context.Context, element *redis.Z) error
	//AddOrUpdateM 如果元素存在则更新元素权重,不存在则增加元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements ...*redis.Z 要添加或更新的带权重的元素
	AddOrUpdateM(ctx context.Context, elements ...*redis.Z) error

	//AddOrUpdate 如果元素存在则更新元素权重,不存在则增加元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params element *redis.Z 要添加或更新的带权重的元素
	AddOrUpdate(ctx context.Context, element *redis.Z) error
	//Reset 重置排名器
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Reset(ctx context.Context) error

	//IncrWeightM 为元素累增一定数值的权重
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elementkey string 元素的key
	//@params weight float64 元素的权重
	IncrWeightM(ctx context.Context, elementkey string, weight float64) (float64, error)
	//IncrWeight 为元素累增1的权重,
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elementkey string 元素的key
	IncrWeight(ctx context.Context, elementkey string) (float64, error)
	//GetWeight 查看元素的权重
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elementkey string 元素的key
	GetWeight(ctx context.Context, elementkey string) (float64, error)
	//RemoveM 删除元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params elements ...interface{} 要删除的元素
	RemoveM(ctx context.Context, elements ...interface{}) error

	//Remove 删除元素
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params element interface{} 要删除的元素
	Remove(ctx context.Context, element interface{}) error

	//读操作

	// Len 获取排名器的当前长度
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Len(ctx context.Context) (int64, error)

	//Range 获取排名范围内的元素,默认全量从小到大排序.
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params opts ...optparams.Option[Rangescopopts]
	Range(ctx context.Context, opts ...optparams.Option[Rangescopopts]) ([]string, error)

	//First 获取排名前若干位的元素,reverse为True则为从大到小否则为从小到大
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params n int64 前几位
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	First(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error)
	//Head first的别名
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params count int64 前几位
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	Head(ctx context.Context, count int64, opts ...optparams.Option[Rangescopopts]) ([]string, error)
	//Last 获取排名后若干位的元素,reverse为True则为从大到小否则为从小到大
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params n int64 后几位
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	Last(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error)

	//Tail Last的别名
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params n int64 后几位
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	Tail(ctx context.Context, n int64, opts ...optparams.Option[Rangescopopts]) ([]string, error)
	//GetRank 获取指定元素的排名,reverse为True则为从大到小否则为从小到大
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params count int64 前几位
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	GetRank(ctx context.Context, element string, opts ...optparams.Option[Rangescopopts]) (int64, error)
	//GetElementByRank 获取指定排名的元素,reverse为True则为从大到小否则为从小到大
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params rank int64 第几名
	//@params opts ...optparams.Option[Rangescopopts] 只有Reverse()会生效
	GetElementByRank(ctx context.Context, rank int64, opts ...optparams.Option[Rangescopopts]) (string, error)
}
