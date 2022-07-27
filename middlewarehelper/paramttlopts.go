package middlewarehelper

import (
	"time"

	"github.com/Golang-Tools/optparams"
)

type RefreshTTLType int8

const (
	RefreshTTLType_NoRefresh RefreshTTLType = iota
	RefreshTTLType_FirstTimeRefresh
	RefreshTTLType_Refresh
)

//RefreshOpt 是否刷新ttl的配置项
type RefreshOpt struct {
	RefreshTTL RefreshTTLType
	TTL        time.Duration
}

//RefreshTTL 设置总是刷新
func RefreshTTL() optparams.Option[RefreshOpt] {
	return optparams.NewFuncOption(func(o *RefreshOpt) {
		o.RefreshTTL = RefreshTTLType_Refresh
	})
}

//WithTTL 设置总是使用指定的ttl刷新key
func WithTTL(t time.Duration) optparams.Option[RefreshOpt] {
	return optparams.NewFuncOption(func(o *RefreshOpt) {
		o.RefreshTTL = RefreshTTLType_Refresh
		o.TTL = t
	})
}

//RefreshTTLAtFirstTime 设置使用MaxTTL在key第一次设置时设置过期
func RefreshTTLAtFirstTime() optparams.Option[RefreshOpt] {
	return optparams.NewFuncOption(func(o *RefreshOpt) {
		o.RefreshTTL = RefreshTTLType_FirstTimeRefresh
	})
}

//WithTTL 设置总是使用指定的ttl刷新key
func WithTTLAtFirstTime(t time.Duration) optparams.Option[RefreshOpt] {
	return optparams.NewFuncOption(func(o *RefreshOpt) {
		o.RefreshTTL = RefreshTTLType_FirstTimeRefresh
		o.TTL = t
	})
}
