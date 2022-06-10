package hypercount

import (
	"context"

	"github.com/Golang-Tools/optparams"
)

//HyperCounterInterface 估算计数器接口
type HyperCounterInterface interface {

	//AddM 添加数据
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params items ...interface{} 添加的数据
	AddM(ctx context.Context, items ...interface{}) error

	//Add 添加数据
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params item interface{} 添加的数据
	Add(ctx context.Context, item interface{}) error

	//Len 检查HyperLogLog中不重复元素的个数
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Len(ctx context.Context) (int64, error)

	//Reset 重置当前hypercount
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Reset(ctx context.Context) error

	//Union 对应set的求并集操作
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params otherhc []*HyperCount 与之做并操作的其他HyperCount对象
	//@params newkeyopts ...optparams.Option[Options] 用于构造新HyperCount对象的参数
	Union(ctx context.Context, otherhc []*HyperCount, newkeyopts ...optparams.Option[Options]) (*HyperCount, error)
}
