package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/lock"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_LOCK_REDIS_URL 测试用的redis地址
const TEST_LOCK_REDIS_URL = "redis://localhost:6379/1"

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackgroundLock(t *testing.T, lockkeyname string, opts ...clientkey.Option) *lock.Lock {
	options, err := redis.ParseURL(TEST_LOCK_REDIS_URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "FlushDB error")
	}
	lockkey := clientkey.New(cli, lockkeyname, opts...)
	lock, err := lock.New(lockkey, "lock1")
	if err != nil {
		assert.FailNow(t, err.Error(), "create lock error")
	}
	return lock
}

func NewBackgroundCache(t *testing.T, lock *lock.Lock, cachekeyname, updatePeriod string, opts ...clientkey.Option) (*Cache, context.Context) {
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "FlushDB error")
	}
	cachekey := clientkey.New(cli, cachekeyname, opts...)
	if err != nil {
		assert.FailNow(t, err.Error(), "create key error")
	}
	cache := New(cachekey, lock, updatePeriod)
	fmt.Println("prepare task done")
	return cache, ctx
}
func Test_new_cache_without_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, "test_cache_lock", clientkey.WithMaxTTL(10*time.Second))
	cache, ctx := NewBackgroundCache(t, lock, "test_cache", "")
	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(5 * time.Second)
	c, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.Equal(t, a, c)
}
func Test_new_cache_with_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, "test_cache_lock", clientkey.WithMaxTTL(2*time.Second))
	cache, ctx := NewBackgroundCache(t, lock, "test_cache", "", clientkey.WithMaxTTL(3*time.Second))
	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		b, err := cache.Get(ctx)
		if err != nil {
			assert.FailNow(t, err.Error(), "cache.Get b error")
		}
		assert.Equal(t, a, b)
	}
}

func Test_new_cache_without_MaxTTL_and_with_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, "test_cache_lock", clientkey.WithMaxTTL(10*time.Second))
	cache, ctx := NewBackgroundCache(t, lock, "test_cache", "*/1  *  *  *  *")
	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})
	err := cache.AutoUpdate()
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.AutoUpdate a error")
	}
	defer cache.StopAutoUpdate()

	a, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(60 * time.Second)
	c, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.NotEqual(t, a, c)
}
