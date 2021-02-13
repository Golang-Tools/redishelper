package cache

import (
	"errors"
)

//ErrArgUpdatePeriodMoreThan1 UpdatePeriod参数的个数超过1个
var ErrArgUpdatePeriodMoreThan1 = errors.New(" updatePeriod 必须只有1位或者没有设置")

//ErrUpdatePeriodLessThan60Second UpdatePeriod小于100微秒
var ErrUpdatePeriodLessThan60Second = errors.New(" updatePeriod 必须不小于60秒")

//ErrUpdateFuncAlreadyRegisted 缓存更新函数已经注册
var ErrUpdateFuncAlreadyRegisted = errors.New("缓存函数已经注册")

//ErrAutoUpdateNeedUpdatePeriod 自动更新需要设置UpdatePeriod
var ErrAutoUpdateNeedUpdatePeriod = errors.New("自动更新需要设置UpdatePeriod")

//ErrAutoUpdateAlreadyStarted 已经启动了自动更新任务
var ErrAutoUpdateAlreadyStarted = errors.New("已经启动了自动更新任务")
