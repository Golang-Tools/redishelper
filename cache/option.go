package cache

import (
	h "github.com/Golang-Tools/redishelper"
)

//Options broker的配置
type Options struct {
	UpdatePeriod string         //使用自动更新,使用crontab格式
	Lock         h.Canlock      //使用的锁
	Limiter      h.CanBeLimiter //使用的限制器
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{}

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

//WithUpdatePeriod 设置定时更新
func WithUpdatePeriod(updatePeriod string) Option {
	return newFuncOption(func(o *Options) {
		o.UpdatePeriod = updatePeriod
	})
}

//WithLock 设置分布式锁防止重复计算
func WithLock(lock h.Canlock) Option {
	return newFuncOption(func(o *Options) {
		o.Lock = lock
	})
}

//WithLimiter 设置分布式限位器
func WithLimiter(limiter h.CanBeLimiter) Option {
	return newFuncOption(func(o *Options) {
		o.Limiter = limiter
	})
}
