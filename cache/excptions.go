package cache

import (
	"errors"
)

//ErrUpdateFuncAlreadyRegisted 缓存更新函数已经注册
var ErrUpdateFuncAlreadyRegisted = errors.New("缓存函数已经注册")

//ErrAutoUpdateNeedUpdatePeriod 自动更新需要设置UpdatePeriod
var ErrAutoUpdateNeedUpdatePeriod = errors.New("自动更新需要设置UpdatePeriod")

//ErrAutoUpdateAlreadyStarted 已经启动了自动更新任务
var ErrAutoUpdateAlreadyStarted = errors.New("已经启动了自动更新任务")
