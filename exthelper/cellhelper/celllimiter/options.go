package celllimiter

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/cellhelper"
	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	"github.com/robfig/cron/v3"
)

type Options struct {
	LimiterOpts []optparams.Option[limiterhelper.Options]
	CellOpts    []optparams.Option[cellhelper.Options]
}

var defaultOptions = Options{
	LimiterOpts: []optparams.Option[limiterhelper.Options]{},
	CellOpts:    []optparams.Option[cellhelper.Options]{},
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

//WithWarningSize 设置警戒水位,必须大于0
func WithWarningSize(warningSize int64) optparams.Option[Options] {
	return l(limiterhelper.WithWarningSize(warningSize))
}

//c 使用optparams.Option[cellhelper.Options]设置limiter配置
func c(opts ...optparams.Option[cellhelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.CellOpts == nil {
			o.CellOpts = []optparams.Option[cellhelper.Options]{}
		}
		o.CellOpts = append(o.CellOpts, opts...)
	})
}

//WithDefaultCountPerPeriod 设置时间间隔内的token消减数
func WithDefaultCountPerPeriod(countPerPeriod int64) optparams.Option[Options] {
	return c(cellhelper.WithDefaultCountPerPeriod(countPerPeriod))
}

//WithDefaultPeriod 设置token消减间隔时长,单位s
func WithDefaultPeriod(period int64) optparams.Option[Options] {
	return c(cellhelper.WithDefaultPeriod(period))
}

//WithMaxTTL 设置token消减间隔时长,单位s
func WithMaxTTL(maxTTL time.Duration) optparams.Option[Options] {
	return c(cellhelper.WithMaxTTL(maxTTL))
}

//WithSpecifiedKey 中间件通用设置,指定使用的键,注意设置key后namespace将失效
func WithSpecifiedKey(key string) optparams.Option[Options] {
	return c(cellhelper.WithSpecifiedKey(key))
}

//WithKey 中间件通用设置,指定使用的键,注意设置后namespace依然有效
func WithKey(key string) optparams.Option[Options] {
	return c(cellhelper.WithKey(key))
}

//WithNamespace 中间件通用设置,指定锁的命名空间
func WithNamespace(ns ...string) optparams.Option[Options] {
	return c(cellhelper.WithNamespace(ns...))
}

//WithAutoRefreshInterval 设置自动刷新过期时间的设置
func WithAutoRefreshInterval(autoRefreshInterval string) optparams.Option[Options] {
	return c(cellhelper.WithAutoRefreshInterval(autoRefreshInterval))
}

//WithTaskCron 设置定时器
func WithTaskCron(taskCron *cron.Cron) optparams.Option[Options] {
	return c(cellhelper.WithTaskCron(taskCron))
}

//WithMaxSize 设置最大水位,必须大于0
func WithMaxSize(maxsize int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.LimiterOpts == nil {
			o.LimiterOpts = []optparams.Option[limiterhelper.Options]{}
		}
		o.LimiterOpts = append(o.LimiterOpts, limiterhelper.WithMaxSize(maxsize))
		if o.CellOpts == nil {
			o.CellOpts = []optparams.Option[cellhelper.Options]{}
		}
		o.CellOpts = append(o.CellOpts, cellhelper.WithDefaultMaxBurst(maxsize-1))
	})
}
