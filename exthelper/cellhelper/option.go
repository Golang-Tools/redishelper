package cellhelper

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/robfig/cron/v3"
)

//Options broker的配置
type Options struct {
	Middlewaretype string
	MaxBurst       int64
	CountPerPeriod int64
	Period         int64
	MiddlewareOpts []optparams.Option[middlewarehelper.Options] //初始化Middleware的配置
}

var Defaultopt = Options{
	Middlewaretype: "redis-ext-cell",
	MaxBurst:       99,
	CountPerPeriod: 30,
	Period:         60,
	MiddlewareOpts: []optparams.Option[middlewarehelper.Options]{},
}

//WithMiddlewaretype 设置中间件类型
func WithMiddlewaretype(typename string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Middlewaretype = typename
	})
}

//WithDefaultMaxBurst 设置最大熔断数,最大token数为MaxBurst+1
func WithDefaultMaxBurst(maxBurst int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.MaxBurst = maxBurst
	})
}

//WithDefaultCountPerPeriod 设置时间间隔内的token消减数
func WithDefaultCountPerPeriod(countPerPeriod int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.CountPerPeriod = countPerPeriod
	})
}

//WithDefaultPeriod 设置token消减间隔时长,单位s
func WithDefaultPeriod(period int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Period = period
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
