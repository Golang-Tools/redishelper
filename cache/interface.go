package cache

import (
	"context"

	"github.com/Golang-Tools/optparams"
)

type CacheInterface interface {
	//RegistUpdateFunc 注册缓存函数到对象
	//@params fn Cachefunc 缓存函数
	RegistUpdateFunc(fn Cachefunc) error
	//ActiveUpdate 主动更新函数,可以被用作定时主动更新,也可以通过监听外部信号来实现更新
	ActiveUpdate()
	//AutoUpdate 自动更新缓存,自动更新缓存内容也受限流器影响,同时锁会用于限制更新程序的执行
	AutoUpdate() error
	//StopAutoUpdate 取消自动更新缓存
	StopAutoUpdate() error
	//Get 获取数据
	//如果缓存中有就从缓存中获取,如果没有则直接从注册的缓存函数中获取,然后将结果更新到缓存
	//如果cache的key有设置MaxTTL则在获取到缓存的数据是会刷新key的过期时间
	//@params ctx context.Context 获取的执行上下文
	//@params opt ...optparams.Option[GetOptions] 获取模式设置
	Get(ctx context.Context, opt ...optparams.Option[GetOptions]) ([]byte, error)
}
