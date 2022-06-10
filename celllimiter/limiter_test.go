package celllimiter

import (
	"context"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/v2/limiterhelper"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

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
func Test_limiter_warningsize_larger_than_max(t *testing.T) {
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	_, err := New(ck, WithMaxTTL(5*time.Second), WithWarningSize(120))
	assert.Equal(t, limiterhelper.ErrLimiterMaxSizeMustLargerThanWaringSize, err)
}

func Test_limiter_interface(t *testing.T) {
	// 准备工作
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	limiter, err := New(ck, WithMaxTTL(5*time.Second), WithMaxSize(120))
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter new get error")
	}
	// 开始测试
	assert.Equal(t, int64(120), limiter.Capacity())
	wl, err := limiter.WaterLevel(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter WaterLevel get error")
	}
	assert.Equal(t, int64(0), wl)
	res, err := limiter.Flood(ctx, 11)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter Flood get error")
	}
	assert.Equal(t, true, res)
	wl, err = limiter.WaterLevel(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter WaterLevel get error")
	}
	assert.Equal(t, int64(11), wl)

	isfull, err := limiter.IsFull(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter IsFull get error")
	}
	assert.Equal(t, false, isfull)
	res, err = limiter.Flood(ctx, 109)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter Flood get error")
	}
	assert.Equal(t, false, res)
	isfull, err = limiter.IsFull(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter IsFull get error")
	}
	assert.Equal(t, true, isfull)

	err = limiter.Reset(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter Reset get error")
	}
	isfull, err = limiter.IsFull(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter IsFull get error")
	}
	assert.Equal(t, false, isfull)
}

func Test_limiter_canExp(t *testing.T) {
	// 准备工作
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	limiter, err := New(ck, WithMaxTTL(5*time.Second), WithSpecifiedKey("test_incrlimiter"))
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter new get error")
	}

	for i := 0; i < 9; i++ {
		res, err := limiter.Flood(ctx, 11)
		if err != nil {
			assert.FailNow(t, err.Error(), "limiter Flood get error")
		}
		assert.Equal(t, true, res)
	}
	for i := 0; i < 9; i++ {
		res, err := limiter.Flood(ctx, 1)
		if err != nil {
			assert.FailNow(t, err.Error(), "limiter Flood get error")
		}
		assert.Equal(t, false, res)
	}
	time.Sleep(10 * time.Second)
	for i := 0; i < 9; i++ {
		res, err := limiter.Flood(ctx, 11)
		if err != nil {
			assert.FailNow(t, err.Error(), "limiter Flood get error")
		}
		assert.Equal(t, true, res)
	}
	for i := 0; i < 9; i++ {
		res, err := limiter.Flood(ctx, 1)
		if err != nil {
			assert.FailNow(t, err.Error(), "limiter Flood get error")
		}
		assert.Equal(t, false, res)
	}
}

func Test_limiter_hooks(t *testing.T) {
	// 准备工作
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	limiter, err := New(ck, WithMaxTTL(5*time.Second), WithSpecifiedKey("test_incrlimiter"))
	limiter.OnFull(func(res, maxsize int64) error {
		assert.GreaterOrEqual(t, res, maxsize)
		return nil
	})
	limiter.OnWarning(func(res, maxsize int64) error {
		assert.GreaterOrEqual(t, maxsize, res)
		return nil
	})
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter new get error")
	}
	_, err = limiter.Flood(ctx, 80)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter Flood get error")
	}
	_, err = limiter.Flood(ctx, 20)
	if err != nil {
		assert.FailNow(t, err.Error(), "limiter Flood get error")
	}
	time.Sleep(time.Second)
}
