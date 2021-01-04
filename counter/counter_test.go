package counter

import (
	"context"
	"testing"

	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

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
	res, err := c.Next(ctx, false)
	if err != nil {
		assert.Error(t, err, "counter next error")
	}
	assert.Equal(t, int64(1), res)
	res, err = c.NextM(ctx, false, 4)
	if err != nil {
		assert.Error(t, err, "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
	err = c.Reset(ctx)
	if err != nil {
		assert.Error(t, err, "counter.reset error")
	}
	res, err = c.Next(ctx, false)
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)

}
