package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/limiter"
	"github.com/Golang-Tools/redishelper/lock"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_LOCK_REDIS_URL 测试用的redis地址
const TEST_LOCK_REDIS_URL = "redis://localhost:6379/1"

//TEST_LIMITER_REDIS_URL 测试用的redis地址
const TEST_LIMITER_REDIS_URL = "redis://localhost:6379/2"

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

//TEST_WRONG_REDIS_URL 测试用的redis地址
const TEST_WRONG_REDIS_URL = "redis://localhost:6380"

func NewBackgroundLimiter(t *testing.T, url, keyname string, opts ...limiter.Option) *limiter.Limiter {
	options, err := redis.ParseURL(url)
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
	key, err := limiter.New(cli, keyname, opts...)
	if err != nil {
		assert.FailNow(t, err.Error(), "create limiter error")
	}
	fmt.Println("prepare limiter done")
	return key
}

func NewBackgroundLock(t *testing.T, url, lockkeyname string, opts ...clientkey.Option) *lock.Lock {
	options, err := redis.ParseURL(url)
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
	fmt.Println("prepare lock done")
	return lock
}

func NewBackground(t *testing.T, url, cachekeyname string, opts ...clientkey.Option) (*clientkey.ClientKey, context.Context) {
	options, err := redis.ParseURL(url)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		// assert.FailNow(t, err.Error(), "FlushDB error")
		log.Info("FlushDB error", log.Dict{"err": err.Error()})
	}
	cachekey := clientkey.New(cli, cachekeyname, opts...)
	return cachekey, ctx
}
func Test_new_cache_without_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, TEST_LOCK_REDIS_URL, "test_cache_lock", clientkey.WithMaxTTL(10*time.Second))
	cachekey, ctx := NewBackground(t, TEST_REDIS_URL, "test_cache")
	cache := New(cachekey, WithLock(lock))
	fmt.Println("prepare task done")

	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(5 * time.Second)
	c, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.Equal(t, a, c)
}
func Test_new_cache_with_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, TEST_LOCK_REDIS_URL, "test_cache_lock", clientkey.WithMaxTTL(2*time.Second))
	cachekey, ctx := NewBackground(t, TEST_REDIS_URL, "test_cache", clientkey.WithMaxTTL(3*time.Second))
	cache := New(cachekey, WithLock(lock))
	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		b, err := cache.Get(ctx, ForceLevelStrict)
		if err != nil {
			assert.FailNow(t, err.Error(), "cache.Get b error")
		}
		assert.Equal(t, a, b)
	}
}

func Test_new_cache_without_MaxTTL_and_with_updatePeriod(t *testing.T) {
	// 准备工作
	lock := NewBackgroundLock(t, TEST_LOCK_REDIS_URL, "test_cache_lock", clientkey.WithMaxTTL(10*time.Second))
	cachekey, ctx := NewBackground(t, TEST_REDIS_URL, "test_cache")
	cache := New(cachekey, WithLock(lock), WithUpdatePeriod("*/1  *  *  *  *"))
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

	a, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(60 * time.Second)
	c, err := cache.Get(ctx, ForceLevelStrict)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.NotEqual(t, a, c)
}

//测试防击穿
func Test_new_cache_with_limiter(t *testing.T) {
	// 准备工作
	// lock := NewBackgroundLock(t, TEST_LOCK_REDIS_URL, "test_cache_lock", clientkey.WithMaxTTL(10*time.Second))
	limiter := NewBackgroundLimiter(t, TEST_LIMITER_REDIS_URL,
		"test_cache_limiter", limiter.WithMaxTTL(180*time.Second), limiter.WithMaxSize(5), limiter.WithWarningSize(3))
	//故意使用错误缓存url测试
	cachekey, ctx := NewBackground(t, TEST_WRONG_REDIS_URL, "test_cache")
	cache := New(cachekey, WithLimiter(limiter), WithUpdatePeriod("*/1  *  *  *  *"))
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

	var latest []byte
	for i := 0; i < 4; i++ {
		a, err := cache.Get(ctx, ForceLevelStrict)
		if err != nil {
			assert.FailNow(t, err.Error(), "cache.Get a error")
		}
		if latest != nil {
			assert.NotEqual(t, a, latest)
		}
		latest = a
	}
	for i := 0; i < 4; i++ {
		_, err := cache.Get(ctx, ForceLevelStrict)
		assert.Equal(t, ErrLimiterNotAllow, err)
	}
}
