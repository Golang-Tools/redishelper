package cache

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	"github.com/Golang-Tools/redishelper/v2/lock"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/robfig/cron/v3"
)

//ForceLevelType 强制执行类型枚举
type ForceLevelType uint16

const (

	//ForceLevel__STRICT 严格模式,无论如何只要报错和不满足组件要求就会终止,当更新函数得到的结果为空时不会放入缓存,而是刷新之前的过期时间
	ForceLevel__STRICT ForceLevelType = iota
	//ForceLevel__CONSTRAINT 约束模式,组件自身失效会继续处理,当更新函数得到的结果为空时会删除缓存以便下次再执行更新操作
	ForceLevel__CONSTRAINT
	//ForceLevel__NOCONSTRAINT 无约束模式,无视组件处理,当更新函数得到的结果为空时不会放入缓存,当更新函数得到的结果为空时依然存入作为缓存
	ForceLevel__NOCONSTRAINT
)

//EmptyResCacheModeType 处理更新函数返回空值的模式
type EmptyResCacheModeType uint16

const (

	//EmptyResCacheMode__IGNORE 当更新函数得到的结果为空时不会放入缓存,而是刷新之前的过期时间保持原有缓存
	EmptyResCacheMode__IGNORE EmptyResCacheModeType = iota
	//EmptyResCacheMode__DELETE 当更新函数得到的结果为空时会删除缓存以便下次再执行更新操作
	EmptyResCacheMode__DELETE
	//EmptyResCacheMode__SAVE 当更新函数得到的结果为空时依然存入作为缓存
	EmptyResCacheMode__SAVE
)

//Options broker的配置
type Options struct {
	UpdatePeriod                string                                       //使用自动更新,使用crontab格式
	QueryAutoUpdateCacheTimeout time.Duration                                //自动更新时写入缓存的超时时长
	Lock                        lock.LockInterface                           //使用的锁
	QueryLockTimeout            time.Duration                                //请求锁的超时时间
	Limiter                     limiterhelper.LimiterInterface               //使用的限制器
	QueryLimiterTimeout         time.Duration                                //请求限流器的超时时间
	EmptyResCacheMode           EmptyResCacheModeType                        //处理更新函数返回空值的模式
	AlwaysRefreshTTL            bool                                         //是否即便没有变化也刷新TTL
	MiddlewareOpts              []optparams.Option[middlewarehelper.Options] //初始化Middleware的配置
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	EmptyResCacheMode:           EmptyResCacheMode__IGNORE,
	QueryAutoUpdateCacheTimeout: 300 * time.Millisecond,
	QueryLockTimeout:            300 * time.Millisecond,
	QueryLimiterTimeout:         300 * time.Millisecond,
	MiddlewareOpts:              []optparams.Option[middlewarehelper.Options]{},
}

//WithUpdatePeriod 设置定时更新
func WithUpdatePeriod(updatePeriod string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.UpdatePeriod = updatePeriod
	})
}

//WithLock 设置分布式锁防止重复计算,分布式锁的作用是限制最小更新间隔
func WithLock(lock lock.LockInterface) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Lock = lock
	})
}

//WithLockTimeout 设置请求分布式锁的过期时间,默认300毫秒
func WithLockTimeout(timeout time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.QueryLockTimeout = timeout
	})
}

//WithLimiter 设置分布式限制器,限制器的作用是设置一段时间内的最大更新次数
func WithLimiter(limiter limiterhelper.LimiterInterface) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Limiter = limiter
	})
}

//WithLimiterTimeout 设置请求限流器的过期时间,默认300毫秒
func WithLimiterTimeout(timeout time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.QueryLimiterTimeout = timeout
	})
}

//WithQueryAutoUpdateCacheTimeout 设置自动更新缓存内容时写入缓存的过期时间,默认300毫秒
func WithQueryAutoUpdateCacheTimeout(timeout time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.QueryAutoUpdateCacheTimeout = timeout
	})
}

//WithAlwaysRefreshTTL设置缓存是否即便没有变化也刷新TTL
func WithAlwaysRefreshTTL() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.AlwaysRefreshTTL = true
	})
}

//m 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func m(opts ...optparams.Option[middlewarehelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.MiddlewareOpts == nil {
			o.MiddlewareOpts = []optparams.Option[middlewarehelper.Options]{}
		}
		o.MiddlewareOpts = append(o.MiddlewareOpts, opts...)
	})
}

//WithSpecifiedKey 中间件通用设置,指定使用的键,注意设置key后namespace将失效
func WithSpecifiedKey(key string) optparams.Option[Options] {
	return m(middlewarehelper.WithSpecifiedKey(key))
}

//WithKey 中间件通用设置,指定使用的键,注意设置后namespace依然有效
func WithKey(key string) optparams.Option[Options] {
	return m(middlewarehelper.WithKey(key))
}

//WithNamespace 中间件通用设置,指定锁的命名空间
func WithNamespace(ns ...string) optparams.Option[Options] {
	return m(middlewarehelper.WithNamespace(ns...))
}

//WithMaxTTL 设置token消减间隔时长,单位s
func WithMaxTTL(maxTTL time.Duration) optparams.Option[Options] {
	return m(middlewarehelper.WithMaxTTL(maxTTL))
}

//WithAutoRefreshInterval 设置自动刷新过期时间的设置
func WithAutoRefreshInterval(autoRefreshInterval string) optparams.Option[Options] {
	return m(middlewarehelper.WithAutoRefreshInterval(autoRefreshInterval))
}

//WithTaskCron 设置定时器
func WithTaskCron(taskCron *cron.Cron) optparams.Option[Options] {
	return m(middlewarehelper.WithTaskCron(taskCron))
}
