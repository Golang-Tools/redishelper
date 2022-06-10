package cache

import (
	"errors"
)

//ErrUpdateFuncAlreadyRegisted 缓存更新函数已经注册
var ErrUpdateFuncAlreadyRegisted = errors.New("update function already registed")

//ErrAutoUpdateNeedUpdatePeriod 自动更新需要设置UpdatePeriod
var ErrAutoUpdateNeedUpdatePeriod = errors.New("auto update need update period")

//ErrAutoUpdateAlreadyStarted 已经启动了自动更新任务
var ErrAutoUpdateAlreadyStarted = errors.New("auto update already started")

//ErrLimiterNotAllow 限制器不允许执行更新任务
var ErrLimiterNotAllow = errors.New("limiter not allow")
