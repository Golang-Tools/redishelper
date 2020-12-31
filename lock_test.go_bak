package redishelper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_lock_Lock(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	lock := proxy.NewLock("testlock", 10)
	locked, err := lock.IsLocked()
	if err != nil {
		assert.Error(t, err, "is locked error")
	}
	assert.Equal(t, false, locked)
	err = lock.Release() //没有定义锁也可以释放,相当于删一个不存在的键
	err = lock.Acquire()
	if err != nil {
		assert.Error(t, err, "lock acquire error")
	}
	err = lock.Acquire()
	if err != ErrLockAlreadySet {
		assert.Error(t, err, "lock acquire error")
	}
	err = lock.Release()
	if err != nil {
		assert.Error(t, err, "lock release error")
	}
}

func Test_lock_waitLock(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	lock := proxy.NewLock("testlock", 10)
	locked, err := lock.IsLocked()
	if err != nil {
		assert.Error(t, err, "is locked error")
	}
	assert.Equal(t, false, locked)
	err = lock.Acquire()
	if err != nil {
		assert.Error(t, err, "lock acquire error")
	}
	locked, err = lock.IsLocked()
	if err != nil {
		assert.Error(t, err, "is locked error")
	}
	assert.Equal(t, true, locked)

	err = lock.Wait(3)
	if err != ErrLockWaitTimeout {
		assert.Error(t, err, "lock wait error")
	}
}
