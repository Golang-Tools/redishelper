//Package key redis的key包装
package clientkey

import (
	"time"

	"github.com/robfig/cron/v3"
)

//Option 设置key行为的选项
//@attribute MaxTTL time.Duration 为0则不设置过期
//@attribute AutoRefresh string 需要为crontab格式的字符串,否则不会自动定时刷新
type Options struct {
	MaxTTL              time.Duration
	AutoRefreshInterval string
	TaskCron            *cron.Cron
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

// func (emptyOption) apply(*Options) {}
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

//WithMaxTTL 设置最大过期时间
func WithMaxTTL(maxttl time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.MaxTTL = maxttl
	})
}

//WithAutoRefreshInterval 设置自动刷新过期时间的设置
func WithAutoRefreshInterval(autoRefreshInterval string) Option {
	return newFuncOption(func(o *Options) {
		o.AutoRefreshInterval = autoRefreshInterval
	})
}

//WithTaskCron 设置定时器
func WithTaskCron(taskCron *cron.Cron) Option {
	return newFuncOption(func(o *Options) {
		o.TaskCron = taskCron
	})
}
