package redishelper

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_lock_Lock(t *testing.T) {
	// 准备工作
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	cli := redis.NewClient(options)
	defer cli.Close()

	ctx := context.Background()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	lock := New(cli, "testlock", "client01", 10*time.Second)
	lock2 := New(cli, "testlock", "client02", 10*time.Second)
	err = lock.Lock(ctx)
	if err != nil {
		assert.Error(t, err, "lock error")
	}
	err = lock.Lock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyLocked)
	}
	err = lock2.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrNoRightToUnLocked)
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.Error(t, err, "该锁已经被解锁")
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyUnLocked)
	}
}

func Test_lock_waitLock(t *testing.T) {
	// 准备工作
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	cli := redis.NewClient(options)
	defer cli.Close()

	ctx := context.Background()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	lock := New(cli, "testlock", "client01", 10*time.Second)
	locked, err := lock.Check(ctx)
	if err != nil {
		assert.Error(t, err, "is locked error")
	}
	assert.Equal(t, false, locked)
}
