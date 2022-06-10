//Package scaner 使用scan遍历查询的工具
package scaner

import "github.com/Golang-Tools/optparams"

//Option 设置key行为的选项
//@attribute MaxTTL time.Duration 为0则不设置过期
//@attribute AutoRefresh string 需要为crontab格式的字符串,否则不会自动定时刷新
type Options struct {
	StepSize int64
}

//WithMaxTTL 设置最大过期时间
func WithSetpSize(stepsize int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.StepSize = stepsize
	})
}
