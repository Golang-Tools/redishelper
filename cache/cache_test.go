package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/idgener"
	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/redishelper/v2/incrlimiter"

	"github.com/Golang-Tools/redishelper/v2/lock"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_LIMITER_REDIS_URL 测试用的redis地址
const TEST_LIMITER_REDIS_URL = "redis://localhost:6379/2"

//TEST_LOCK_REDIS_URL 测试用的redis地址
const TEST_LOCK_REDIS_URL = "redis://localhost:6379/1"

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

//TEST_WRONG_REDIS_URL 测试用的redis地址
const TEST_WRONG_REDIS_URL = "redis://localhost:6380"

func NewBackgroundLimiter(t *testing.T) (*incrlimiter.Limiter, redis.UniversalClient) {
	options, err := redis.ParseURL(TEST_LIMITER_REDIS_URL)
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
	limiter, err := incrlimiter.New(cli,
		incrlimiter.WithSpecifiedKey("test_cache_limiter"),
		incrlimiter.WithMaxTTL(180*time.Second),
		incrlimiter.WithMaxSize(5),
		incrlimiter.WithWarningSize(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "create limiter error")
	}
	fmt.Println("prepare limiter done")
	return limiter, cli
}

func NewBackgroundLock(t *testing.T) (*lock.Lock, redis.UniversalClient) {
	options, err := redis.ParseURL(TEST_LIMITER_REDIS_URL)
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

	lock, err := lock.New(cli, lock.WithSpecifiedKey("test_cache_lock"), lock.WithMaxTTL(10*time.Second), lock.WithClientID("cache_lock"))
	if err != nil {
		assert.FailNow(t, err.Error(), "create lock error")
	}
	return lock, cli
}

func NewBackgroundClient(t *testing.T) (redis.UniversalClient, context.Context) {
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
	return cli, ctx
}
func NewWrongBackgroundClient(t *testing.T) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(TEST_WRONG_REDIS_URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "FlushDB error")
		log.Info("FlushDB error", map[string]any{"err": err.Error()})
	}
	return cli, ctx
}
func Test_new_cache_without_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock, cli1 := NewBackgroundLock(t)
	defer cli1.Close()
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()
	cache, err := New(cli, WithSpecifiedKey("test_cache"), WithLock(lock))
	if err != nil {
		assert.FailNow(t, err.Error(), "new cache error")
	}
	fmt.Println("prepare task done")

	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := idgener.Next(idgener.IDGEN_UUIDV4)
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(5 * time.Second)
	c, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.Equal(t, a, c)
}

func Test_new_cache_with_MaxTTL_and_without_updatePeriod(t *testing.T) {
	// 准备工作
	lock, cli1 := NewBackgroundLock(t)
	defer cli1.Close()
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()
	cache, err := New(cli, WithSpecifiedKey("test_cache"), WithLock(lock), WithMaxTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "new cache error")
	}
	fmt.Println("prepare task done")

	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := idgener.Next(idgener.IDGEN_UUIDV4)
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		b, err := cache.Get(ctx, StrictMode())
		if err != nil {
			assert.FailNow(t, err.Error(), "cache.Get b error")
		}
		assert.Equal(t, a, b)
	}
}

func Test_new_cache_without_MaxTTL_and_with_updatePeriod(t *testing.T) {
	// 准备工作
	lock, cli1 := NewBackgroundLock(t)
	defer cli1.Close()
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()
	cache, err := New(cli, WithSpecifiedKey("test_cache"), WithLock(lock), WithUpdatePeriod("*/1  *  *  *  *"))
	if err != nil {
		assert.FailNow(t, err.Error(), "new cache error")
	}
	fmt.Println("prepare task done")

	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := idgener.Next(idgener.IDGEN_UUIDV4)
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	err = cache.AutoUpdate()
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.AutoUpdate a error")
	}
	defer cache.StopAutoUpdate()

	a, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
	time.Sleep(60 * time.Second)
	c, err := cache.Get(ctx, StrictMode())
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get c error")
	}
	assert.NotEqual(t, a, c)
}

//测试防击穿
func Test_new_cache_with_limiter(t *testing.T) {
	// 准备工作
	// lock, cli1 := NewBackgroundLock(t)
	// defer cli1.Close()
	limiter, cli2 := NewBackgroundLimiter(t)
	defer cli2.Close()
	cli, ctx := NewWrongBackgroundClient(t)
	defer cli.Close()
	cache, err := New(cli, WithSpecifiedKey("test_cache"), WithLimiter(limiter), WithUpdatePeriod("*/1  *  *  *  *"))
	if err != nil {
		assert.FailNow(t, err.Error(), "new cache error")
	}
	fmt.Println("prepare task done")

	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := idgener.Next(idgener.IDGEN_UUIDV4)
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	err = cache.AutoUpdate()
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.AutoUpdate a error")
	}
	defer cache.StopAutoUpdate()

	var latest []byte
	for i := 0; i < 4; i++ {
		a, err := cache.Get(ctx, StrictMode())
		if err != nil {
			assert.FailNow(t, err.Error(), fmt.Sprintf("cache.Get a error @ time %d", i))
		}
		if latest != nil {
			assert.NotEqual(t, a, latest)
		}
		latest = a
	}
	for i := 0; i < 4; i++ {
		_, err := cache.Get(ctx, StrictMode())
		assert.Equal(t, ErrLimiterNotAllow, err)
	}
}
