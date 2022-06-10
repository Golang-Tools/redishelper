//Package limiterhelper 限制器帮助模块,用于快速构造限制器
//限制器可以用于防止短时间内大量请求同时处理,比如缓存防击穿,防爬虫等
package limiterhelper

import "context"

//钩子函数
//@params res int64   当前水位
//@params maxsize int64 最大水位
type Hook func(res, maxsize int64) error

//LimiterInterface 限制器接口
type LimiterInterface interface {
	//注水,返回值true表示注水成功,false表示满了无法注水,抛出异常返回true
	Flood(context.Context, int64) (bool, error)
	//水位
	WaterLevel(context.Context) (int64, error)
	//容量
	Capacity() int64
	//是否容量已满
	IsFull(context.Context) (bool, error)
	//重置
	Reset(context.Context) error
	//注册到警戒水位的钩子,钩子会在执行Flood方法时触发
	OnWarning(fn Hook) error
	//注册水位满时的钩子,钩子会在执行Flood方法时触发
	OnFull(fn Hook) error
}
