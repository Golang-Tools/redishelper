package key

import (
	"errors"
)

//ErrKeyNotSetMaxTLL key没有设置最大tll
var ErrKeyNotSetMaxTLL = errors.New("key没有设置最大tll")

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrParamDelimiterLengthMustLessThan2 不定长参数delimiter长度必须小于2
var ErrParamDelimiterLengthMustLessThan2 = errors.New("不定长参数delimiter长度必须小于2")

//ErrAutoRefreshTaskHasBeenSet 已经启动了自动刷新任务
var ErrAutoRefreshTaskHasBeenSet = errors.New("已经启动了自动刷新任务")

//ErrAutoRefreshTaskHNotSetYet 自动刷新任务未启动
var ErrAutoRefreshTaskHNotSetYet = errors.New("自动刷新任务未启动")

//ErrAutoRefreshTaskInterval 未设置自动刷新任务的间隔
var ErrAutoRefreshTaskInterval = errors.New("未设置自动刷新任务的间隔")
