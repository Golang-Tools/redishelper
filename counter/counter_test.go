package counter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, keyname string, opt *clientkey.Option) (*clientkey.ClientKey, context.Context) {
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
	key, err := clientkey.New(cli, keyname, opt)
	if err != nil {
		assert.FailNow(t, err.Error(), "create key error")
	}
	fmt.Println("prepare task done")
	return key, ctx
}

func Test_counter_TTL(t *testing.T) {
	// 准备工作
	key, ctx := NewBackground(t, "test_bitmap", &clientkey.Option{
		MaxTTL: 2 * time.Second,
	})
	defer key.Client.Close()

	c := New(key)

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
	res, err = c.NextM(ctx, 3)
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
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, clientkey.ErrKeyNotExist)
	} else {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
}

func Test_counter_counter(t *testing.T) {
	// 准备工作
	key, ctx := NewBackground(t, "test_counter", nil)
	defer key.Client.Close()
	c := New(key)

	//开始测试
	res, err := c.Next(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter next error")
	}
	assert.Equal(t, int64(1), res)
	res, err = c.NextM(ctx, 4)
	if err != nil {
		assert.FailNow(t, err.Error(), "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
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
