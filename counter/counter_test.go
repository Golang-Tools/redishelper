package counter

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_counter_TTL(t *testing.T) {
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
	c := New(cli, "test_counter", 2*time.Second)

	//开始测试
	res, err := c.Next(ctx)
	if err != nil {
		assert.Error(t, err, "counter next error")
	}
	assert.Equal(t, int64(1), res)
	ttl, err := c.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	log.Info("set ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(2*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "counter error")
	}
	log.Info("get ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	res, err = c.NextM(ctx, 3)
	if err != nil {
		assert.Error(t, err, "counter nextM error")
	}
	assert.Equal(t, int64(4), res)
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "counter error")
	}
	log.Info("get ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(2*time.Second), int64(ttl))
	time.Sleep(3 * time.Second)
	ttl, err = c.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, ErrKeyNotExist)
	} else {
		assert.Error(t, errors.New("Bitmap.IsSetted error"))
	}
}

func Test_counter_counter(t *testing.T) {
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
	c := New(cli, "test_counter")

	//开始测试
	res, err := c.Next(ctx)
	if err != nil {
		assert.Error(t, err, "counter next error")
	}
	assert.Equal(t, int64(1), res)
	res, err = c.NextM(ctx, 4)
	if err != nil {
		assert.Error(t, err, "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
	err = c.Reset(ctx)
	if err != nil {
		assert.Error(t, err, "counter.reset error")
	}
	res, err = c.Next(ctx)
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)
}
