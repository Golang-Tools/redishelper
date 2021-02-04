package lock

import (
	"errors"
)

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrLockNotSetMaxTLL lock没有设置最大tll
var ErrLockNotSetMaxTLL = errors.New("lock没有设置最大tll")

//ErrAlreadyLocked 该锁已经被锁定
var ErrAlreadyLocked = errors.New("该锁已经被锁定")

//ErrAlreadyUnLocked 该锁已经被解锁
var ErrAlreadyUnLocked = errors.New("该锁已经被解锁")

//ErrNoRightToUnLocked 无权解锁该锁
var ErrNoRightToUnLocked = errors.New("无权解锁该锁")

//ErrArgCheckPeriodMoreThan1 checkperiod参数的个数超过1个
var ErrArgCheckPeriodMoreThan1 = errors.New("checkperiod 必须只有1位或者没有设置")

//ErrCheckPeriodLessThan100Microsecond checkperiod小于100微秒
var ErrCheckPeriodLessThan100Microsecond = errors.New("checkperiod 必须不小于100微秒")
