package clientkeybatch

import (
	"errors"
)

//ErrBatchNotSetMaxTLL batch没有设置最大tll
var ErrBatchNotSetMaxTLL = errors.New("batch没有设置最大tll")

//ErrKeysMustMoreThanOne keys参数个数必须大于0
var ErrKeysMustMoreThanOne = errors.New("keys参数个数必须大于0")

//ErrKeysMustSameClient keys必须使用相同的客户端对象
var ErrKeysMustSameClient = errors.New("keys必须使用相同的客户端对象")

//ErrKeysMustSameType keys必须为相同的类型
var ErrKeysMustSameType = errors.New("keys必须为相同的类型")

//ErrAutoRefreshTaskHasBeenSet 已经启动了自动刷新任务
var ErrAutoRefreshTaskHasBeenSet = errors.New("已经启动了自动刷新任务")

//ErrAutoRefreshTaskHNotSetYet 自动刷新任务未启动
var ErrAutoRefreshTaskHNotSetYet = errors.New("自动刷新任务未启动")

//ErrAutoRefreshTaskInterval 未设置自动刷新任务的间隔
var ErrAutoRefreshTaskInterval = errors.New("未设置自动刷新任务的间隔")
