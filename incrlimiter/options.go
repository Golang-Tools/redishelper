package incrlimiter

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/robfig/cron/v3"
)

type Options struct {
	LimiterOpts    []optparams.Option[limiterhelper.Options]
	MiddlewareOpts []optparams.Option[middlewarehelper.Options] //初始化Middleware的配置
}

var defaultOptions = Options{
	LimiterOpts:    []optparams.Option[limiterhelper.Options]{},
	MiddlewareOpts: []optparams.Option[middlewarehelper.Options]{},
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

//l 使用optparams.Option[limiterhelper.Options]设置limiter配置
func l(opts ...optparams.Option[limiterhelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.LimiterOpts == nil {
			o.LimiterOpts = []optparams.Option[limiterhelper.Options]{}
		}
		o.LimiterOpts = append(o.LimiterOpts, opts...)
	})
}

//WithMaxSize 设置最大水位,必须大于0
func WithMaxSize(maxsize int64) optparams.Option[Options] {
	return l(limiterhelper.WithMaxSize(maxsize))
}

//WithWarningSize 设置警戒水位,必须大于0
func WithWarningSize(warningSize int64) optparams.Option[Options] {
	return l(limiterhelper.WithWarningSize(warningSize))
}
