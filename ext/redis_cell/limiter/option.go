package limiter

import (
	"github.com/Golang-Tools/redishelper/ext/redis_cell"
)

//钩子函数
type Hook func(res *redis_cell.RedisCellStatus) error

//Options broker的配置
type Options struct {
	WarningSize  int64
	AsyncHooks   bool
	WarningHooks []Hook
	FullHooks    []Hook
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	WarningSize:  80,
	AsyncHooks:   false,
	WarningHooks: []Hook{},
	FullHooks:    []Hook{},
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

type funcOption struct {
	f func(*Options)
}

func (fo *funcOption) Apply(do *Options) {
	fo.f(do)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

//WithWarningSize 设置警戒水位,必须大于0
func WithWarningSize(warningSize int64) Option {
	return newFuncOption(func(o *Options) {
		if warningSize > 0 {
			o.WarningSize = warningSize
		}
	})
}

//WithWarningHook 设置警戒水位钩子
func WithWarningHook(hook Hook) Option {
	return newFuncOption(func(o *Options) {
		o.WarningHooks = append(o.WarningHooks, hook)
	})
}

//WithFullHook 设置水满时触发的钩子
func WithFullHook(hook Hook) Option {
	return newFuncOption(func(o *Options) {
		o.FullHooks = append(o.FullHooks, hook)
	})
}

//WithRunHooksAsync 设置钩子并发执行
func WithAsyncHooks() Option {
	return newFuncOption(func(o *Options) {
		o.AsyncHooks = true
	})
}
