package cellhelper

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
)

//ClThrottleOpt ClThrottle的配置
type ClThrottleOpts struct {
	RefreshOpts    []optparams.Option[middlewarehelper.RefreshOpt]
	MaxBurst       int64
	CountPerPeriod int64
	Period         int64
}

//WithMaxBurst 设置最大熔断数,最大token数为MaxBurst+1
func WithMaxBurst(maxBurst int64) optparams.Option[ClThrottleOpts] {
	return optparams.NewFuncOption(func(o *ClThrottleOpts) {
		o.MaxBurst = maxBurst
	})
}

//WithCountPerPeriod 设置时间间隔内的token消减数
func WithCountPerPeriod(countPerPeriod int64) optparams.Option[ClThrottleOpts] {
	return optparams.NewFuncOption(func(o *ClThrottleOpts) {
		o.CountPerPeriod = countPerPeriod
	})
}

//WithPeriod 设置token消减间隔时长,单位s
func WithPeriod(period int64) optparams.Option[ClThrottleOpts] {
	return optparams.NewFuncOption(func(o *ClThrottleOpts) {
		o.Period = period
	})
}

//mAdd 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mAdd(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[ClThrottleOpts] {
	return optparams.NewFuncOption(func(o *ClThrottleOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//RefreshTTL 设置总是刷新,该参数对pipeline无效
func RefreshTTL() optparams.Option[ClThrottleOpts] {
	return mAdd(middlewarehelper.RefreshTTL())
}

//WithTTL 设置总是使用指定的ttl刷新key,该参数对pipeline无效
func WithTTL(t time.Duration) optparams.Option[ClThrottleOpts] {
	return mAdd(middlewarehelper.WithTTL(t))
}

//RefreshTTLAtFirstTime 设置使用MaxTTL在key第一次设置时设置过期,该参数对pipeline无效
func RefreshTTLAtFirstTime() optparams.Option[ClThrottleOpts] {
	return mAdd(middlewarehelper.RefreshTTLAtFirstTime())
}

//WithTTL 设置总是使用指定的ttl刷新key
func WithTTLAtFirstTime(t time.Duration) optparams.Option[ClThrottleOpts] {
	return mAdd(middlewarehelper.WithTTLAtFirstTime(t))
}
