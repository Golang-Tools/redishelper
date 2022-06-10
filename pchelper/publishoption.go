package pchelper

import (
	"github.com/Golang-Tools/optparams"
)

//PublishOptions 消费端listen方法的配置
type PublishOptions struct {
	NoMkStream bool
	MinID      string
	ID         string
	Limit      int64
	MaxLen     int64 //stream生产者专用,用于设置流的最大长度
	Strict     bool  //stream生产者专用,用于设置流是否为严格模式
}

var DefaultPublishOpt = PublishOptions{}

//WithNoMkStream stream专用
func WithNoMkStream() optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.NoMkStream = true
	})
}

//WithID stream专用
func WithID(id string) optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.ID = id
	})
}

//WithMinID stream专用
func WithMinID(minid string) optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.MinID = minid
	})
}

//WithMinID stream专用
func WithLimit(limit int64) optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.Limit = limit
	})
}

func WithMaxlen(n int64) optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.MaxLen = n
	})
}

func WithStrictMode() optparams.Option[PublishOptions] {
	return optparams.NewFuncOption(func(o *PublishOptions) {
		o.Strict = true
	})
}
