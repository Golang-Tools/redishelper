package lock

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/robfig/cron/v3"
)

type Options struct {
	CheckPeriod    time.Duration                                //等待的轮询间隔
	MiddlewareOpts []optparams.Option[middlewarehelper.Options] //初始化Middleware的配置
	ClientIDOpts   []optparams.Option[clientIdhelper.Options]   //初始化clientID的配置
}

var defaultOptions = Options{
	CheckPeriod:    500 * time.Microsecond,
	MiddlewareOpts: []optparams.Option[middlewarehelper.Options]{},
	ClientIDOpts:   []optparams.Option[clientIdhelper.Options]{},
}

//WithCheckPeriod 锁的设置项,设置检查锁状态的轮询间隔时间
func WithCheckPeriod(checkperiod time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.CheckPeriod = checkperiod
	})
}

//m 使用optparams.Option[clientIdhelper.Options]设置客户端ID属性
func c(opts ...optparams.Option[clientIdhelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ClientIDOpts == nil {
			o.ClientIDOpts = []optparams.Option[clientIdhelper.Options]{}
		}
		o.ClientIDOpts = append(o.ClientIDOpts, opts...)
	})
}

//WithSpecifiedKey 中间件通用设置,指定使用的键,注意设置key后namespace将失效
func WithClientID(key string) optparams.Option[Options] {
	return c(clientIdhelper.WithClientID(key))
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
