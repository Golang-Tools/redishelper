package counterhelper

import "context"

type CounterInterface interface {
	//NextN 加m后的当前计数
	//如果设置了MaxTTL则会在执行好后刷新TTL
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params value int64 要增加的值.这个值可以为负
	NextN(ctx context.Context, value int64) (int64, error)

	//Next 加1后的当前计数值
	//如果设置了MaxTTL则会在执行好后刷新TTL
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Next(ctx context.Context) (int64, error)
	//Len 当前的计数量
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Len(ctx context.Context) (int64, error)
	//Reset 重置当前计数器
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Reset(ctx context.Context) error
}
