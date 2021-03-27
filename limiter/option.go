package limiter

import (
	"time"
)

//钩子函数
type Hook func(res, maxsize int64) error

//Options broker的配置
type Options struct {
	MaxSize      int64
	WarningSize  int64
	MaxTTL       time.Duration
	AsyncHooks   bool
	WarningHooks []Hook
	FullHooks    []Hook
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	MaxSize:      100,
	WarningSize:  80,
	MaxTTL:       60 * time.Second,
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

//WithMaxSize 设置最大水位,必须大于0
func WithMaxSize(maxsize int64) Option {
	return newFuncOption(func(o *Options) {
		if maxsize > 0 {
			o.MaxSize = maxsize
		}
	})
}

//WithWarningSize 设置警戒水位,必须大于0
func WithWarningSize(warningSize int64) Option {
	return newFuncOption(func(o *Options) {
		if warningSize > 0 {
			o.WarningSize = warningSize
		}
	})
}

//WithMaxTTL 设置最大过期时间
func WithMaxTTL(maxTTL time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.MaxTTL = maxTTL
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
