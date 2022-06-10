package lock

import (
	"errors"
)

//ErrAlreadyLocked 该锁已经被锁定
var ErrAlreadyLocked = errors.New("already locked")

//ErrAlreadyUnLocked 该锁已经被解锁
var ErrAlreadyUnLocked = errors.New("already unlocked")

//ErrNoRightToUnLock 无权解锁该锁
var ErrNoRightToUnLock = errors.New("no right to unlock")

//ErrCheckPeriodLessThan100Microsecond checkperiod小于100微秒
var ErrCheckPeriodLessThan100Microsecond = errors.New("checkperiod less than 100 microsecond")
