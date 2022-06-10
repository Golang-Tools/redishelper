package middlewarehelper

import (
	"time"

	"github.com/Golang-Tools/namespace"
	"github.com/Golang-Tools/optparams"
	"github.com/robfig/cron/v3"
)

type Options struct {
	Key                 string              //锁使用的key,如果为空则随机创建
	OnlyKey             bool                //只设置key不设置namespace
	Namespace           namespace.NameSpcae //指定命名空间
	MaxTTL              time.Duration
	AutoRefreshInterval string
	TaskCron            *cron.Cron
}

var DefaultOptions = Options{
	Namespace: namespace.NameSpcae{"redishelper"},
}

//WithSpecifiedKey 中间件通用设置,指定使用的键,注意设置key后namespace将失效
func WithSpecifiedKey(key string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Key = key
		o.OnlyKey = true
	})
}

//WithKey 中间件通用设置,指定使用的键,注意设置后namespace依然有效
func WithKey(key string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Key = key
		o.OnlyKey = false
	})
}

//WithNamespace 中间件通用设置,指定锁的命名空间
func WithNamespace(ns ...string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Namespace = namespace.NameSpcae(ns)
	})
}

//WithMaxTTL 设置token消减间隔时长,单位s
func WithMaxTTL(maxTTL time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.MaxTTL = maxTTL
	})
}

//WithAutoRefreshInterval 设置自动刷新过期时间的设置
func WithAutoRefreshInterval(autoRefreshInterval string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.AutoRefreshInterval = autoRefreshInterval
	})
}

//WithTaskCron 设置定时器
func WithTaskCron(taskCron *cron.Cron) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.TaskCron = taskCron
	})
}
