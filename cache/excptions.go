package cache

import (
	"errors"
)

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrCacheNotSetMaxTLL cache没有设置最大tll
var ErrCacheNotSetMaxTLL = errors.New("cache没有设置最大tll")

//ErrArgUpdatePeriodMoreThan1 UpdatePeriod参数的个数超过1个
var ErrArgUpdatePeriodMoreThan1 = errors.New(" updatePeriod 必须只有1位或者没有设置")

//ErrUpdatePeriodLessThan60Second UpdatePeriod小于100微秒
var ErrUpdatePeriodLessThan60Second = errors.New(" updatePeriod 必须不小于60秒")
