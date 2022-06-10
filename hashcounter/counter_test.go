package hashcounter

import (
	"context"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
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
func Test_counter_TTL(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()

	c, err := New(cli, "test_field", WithSpecifiedKey("test_hashcounter"), WithMaxTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "New get error")
	}

	//开始测试
	res, err := c.Next(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter next error")
	}
	assert.Equal(t, int64(1), res)
	ttl, err := c.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(1*time.Second))
	assert.LessOrEqual(t, int64(0*time.Second), int64(ttl))
	//NextM可以刷新key
	res, err = c.NextN(ctx, 3)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter nextM error")
	}
	assert.Equal(t, int64(4), res)
	time.Sleep(1 * time.Second)
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))

	//结束
	time.Sleep(3 * time.Second)
	_, err = c.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, middlewarehelper.ErrKeyNotExists)
	} else {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
}

func Test_counter_counter(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()

	c, err := New(cli, "test_field", WithSpecifiedKey("test_hashcounter"))
	if err != nil {
		assert.FailNow(t, err.Error(), "New get error")
	}

	//开始测试
	res, err := c.Next(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter next error")
	}
	assert.Equal(t, int64(1), res)
	//测试 nextM
	res, err = c.NextN(ctx, 4)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
	//测试 len
	res, err = c.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)

	//测试reset
	err = c.Reset(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter.reset error")
	}
	res, err = c.Next(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter.Count error")
	}
	assert.Equal(t, int64(1), res)
}
