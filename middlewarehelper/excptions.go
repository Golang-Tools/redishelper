package middlewarehelper

import (
	"errors"
)

//ErrKeyNotExists key不存在
var ErrKeyNotExists = errors.New("key not exists")

//ErrKeyNotSetExpire key没有设置过期
var ErrKeyNotSetExpire = errors.New("key not set expire")

//ErrKeyNotSetMaxTLL key没有设置最大tll
var ErrKeyNotSetMaxTLL = errors.New("key not set MaxTLL ")

//ErrAutoRefreshTaskHasBeenSet 已经启动了自动刷新任务
var ErrAutoRefreshTaskHasBeenSet = errors.New("autoRefresh task has been set")

//ErrAutoRefreshTaskNotSetYet 自动刷新任务未启动
var ErrAutoRefreshTaskNotSetYet = errors.New("autoRefresh task not set yet")

//ErrAutoRefreshTaskInterval 未设置自动刷新任务的间隔
var ErrAutoRefreshTaskInterval = errors.New("autoRefresh task interval not set")
