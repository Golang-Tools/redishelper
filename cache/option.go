package cache

import (
	h "github.com/Golang-Tools/redishelper"
)

//ForceLevelType 强制执行类型枚举
type ForceLevelType uint16

const (

	//ForceLevel__STRICT 严格模式,无论如何只要报错和不满足组件要求就会终止,当更新函数得到的结果为空时不会放入缓存,而是刷新之前的过期时间
	ForceLevel__STRICT ForceLevelType = iota
	//ForceLevel__CONSTRAINT 约束模式,组件自身失效会继续处理,当更新函数得到的结果为空时会删除缓存以便下次再执行更新操作
	ForceLevel__CONSTRAINT
	//ForceLevel__NOCONSTRAINT 无约束模式,无视组件处理,当更新函数得到的结果为空时不会放入缓存,当更新函数得到的结果为空时依然存入作为缓存
	ForceLevel__NOCONSTRAINT
)

//EmptyResCacheModeType 处理更新函数返回空值的模式
type EmptyResCacheModeType uint16

const (

	//EmptyResCacheMode__IGNORE 当更新函数得到的结果为空时不会放入缓存,而是刷新之前的过期时间保持原有缓存
	EmptyResCacheMode__IGNORE EmptyResCacheModeType = iota
	//EmptyResCacheMode__DELETE 当更新函数得到的结果为空时会删除缓存以便下次再执行更新操作
	EmptyResCacheMode__DELETE
	//EmptyResCacheMode__SAVE 当更新函数得到的结果为空时依然存入作为缓存
	EmptyResCacheMode__SAVE
)

//Options broker的配置
type Options struct {
	UpdatePeriod      string                //使用自动更新,使用crontab格式
	Lock              h.Canlock             //使用的锁
	Limiter           h.CanBeLimiter        //使用的限制器
	EmptyResCacheMode EmptyResCacheModeType //处理更新函数返回空值的模式
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	EmptyResCacheMode: EmptyResCacheMode__IGNORE,
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

//WithUpdatePeriod 设置定时更新
func WithUpdatePeriod(updatePeriod string) Option {
	return newFuncOption(func(o *Options) {
		o.UpdatePeriod = updatePeriod
	})
}

//WithLock 设置分布式锁防止重复计算,分布式锁的作用是限制最小更新间隔
func WithLock(lock h.Canlock) Option {
	return newFuncOption(func(o *Options) {
		o.Lock = lock
	})
}

//WithLimiter 设置分布式限制器,限制器的作用是设置一段时间内的最大更新次数
func WithLimiter(limiter h.CanBeLimiter) Option {
	return newFuncOption(func(o *Options) {
		o.Limiter = limiter
	})
}
