package limiterhelper

import (
	"sync"

	"github.com/Golang-Tools/optparams"
)

//LimiterABC 分布式限制器公用组件
type LimiterABC struct {
	WarningHook Hook
	FullHook    Hook
	Hookslock   sync.RWMutex
	Opt         Options
}

func New(opts ...optparams.Option[Options]) (*LimiterABC, error) {
	l := new(LimiterABC)
	l.Opt = Defaultopt
	optparams.GetOption(&l.Opt, opts...)
	if l.Opt.MaxSize <= l.Opt.WarningSize {
		return nil, ErrLimiterMaxSizeMustLargerThanWaringSize
	}
	return l, nil
}

//OnWarning 注册在到警戒水位时的钩子
//@params fn Hook 钩子函数
func (c *LimiterABC) OnWarning(fn Hook) error {
	c.Hookslock.Lock()
	defer c.Hookslock.Unlock()
	if c.WarningHook != nil {
		return ErrAlreadyHasHook
	}
	c.WarningHook = fn

	return nil
}

//OnFull 注册在到水位漫时的钩子
//@params fn Hook 钩子函数
func (c *LimiterABC) OnFull(fn Hook) error {
	c.Hookslock.Lock()
	defer c.Hookslock.Unlock()
	if c.FullHook != nil {
		return ErrAlreadyHasHook
	}
	c.FullHook = fn
	return nil
}

//CheckWaterline 检查水位是否已满
//@params size int64 当前水位
//@returns bool 是否水位已满
func (l *LimiterABC) CheckWaterline(size int64, blocked bool) bool {
	if blocked {
		if l.FullHook != nil {
			l.FullHook(size, l.Opt.MaxSize)
		}
		return true
	}
	if size >= l.Opt.MaxSize {
		if l.FullHook != nil {
			l.FullHook(size, l.Opt.MaxSize)
		}
		return true
	} else {
		if size >= l.Opt.WarningSize {
			if l.WarningHook != nil {
				l.WarningHook(size, l.Opt.MaxSize)
			}
		}
	}
	return false
}

//Capacity 限制器最大容量
func (l *LimiterABC) Capacity() int64 {
	return l.Opt.MaxSize
}
