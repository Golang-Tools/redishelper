package limiterhelper

import (
	"github.com/Golang-Tools/optparams"
)

//Options broker的配置
type Options struct {
	MaxSize     int64
	WarningSize int64
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	MaxSize:     100,
	WarningSize: 80,
}

//WithMaxSize 设置最大水位,必须大于0
func WithMaxSize(maxsize int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if maxsize > 0 {
			o.MaxSize = maxsize
		}
	})
}

//WithWarningSize 设置警戒水位,必须大于0
func WithWarningSize(warningSize int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if warningSize > 0 {
			o.WarningSize = warningSize
		}
	})
}
