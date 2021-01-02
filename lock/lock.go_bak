package redishelper

import (
	"time"
)

// distributedLock 分布式锁结构
type distributedLock struct {
	proxy   *redisHelper
	key     string
	timeout int64
}

func newLock(proxy *redisHelper, key string, timeout int64) *distributedLock {
	lock := new(distributedLock)
	lock.key = key
	lock.timeout = timeout
	lock.proxy = proxy
	return lock
}

// Release 释放锁,锁不存在也不会报错
func (lock *distributedLock) Release() error {
	if !lock.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := lock.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.Del(lock.key).Result()
	if err != nil {
		return err
	}
	return nil
}

// SetLock 设置锁
func (lock *distributedLock) Acquire() error {
	if !lock.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := lock.proxy.GetConn()
	if err != nil {
		return err
	}
	set, err := conn.SetNX(lock.key, "1", time.Duration(lock.timeout)*time.Second).Result()
	if err != nil {
		return err
	}
	if set == false {
		return ErrLockAlreadySet
	}
	return nil
}

//IsLocked 判断锁是否存在
func (lock *distributedLock) IsLocked() (bool, error) {
	if !lock.proxy.IsOk() {
		return false, ErrHelperNotInited
	}
	conn, err := lock.proxy.GetConn()
	if err != nil {
		return false, err
	}
	exist, err := conn.Exists(lock.key).Result()
	if err != nil {
		return false, err
	}
	if exist == 0 {
		return false, nil
	}
	return true, nil
}

//Wait 等待锁解锁
func (lock *distributedLock) Wait(timeout int64) error {
	ch := make(chan error)
	go func() {
		for {
			locked, err := lock.IsLocked()
			if err != nil {
				ch <- err
				break
			} else {
				if !locked {
					ch <- Done
					break
				} else {
					time.Sleep(time.Duration(100) * time.Microsecond)
				}
			}
		}
	}()
	select {
	case res := <-ch:
		{
			if res == Done {
				return nil
			}
			return res
		}
	case <-time.After(time.Duration(timeout) * time.Second):
		return ErrLockWaitTimeout

	}
}
