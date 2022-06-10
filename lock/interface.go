package lock

import "context"

//LockInterface 锁对象的接口
type LockInterface interface {
	Lock(context.Context) error
	Unlock(context.Context) error
	Wait(context.Context) error
}
