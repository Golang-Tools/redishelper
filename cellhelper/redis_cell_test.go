package cellhelper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, URL string) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	fmt.Println("prepare task done")
	return cli, ctx
}

//Test_KeyspaceNotification_Sync 测试同步配置
func Test_ClThrottle(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cell"
	cell, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	res, err := cell.ClThrottle(ctx, 1)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	fmt.Println(res)
	assert.Equal(t, res.Blocked, false)
	assert.Equal(t, res.Max, cell.opt.MaxBurst+1)
}

func Test_ClThrottle_WithMaxTTL(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cell"
	cell, err := New(cli, WithSpecifiedKey(key), WithMaxTTL(1*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	res, err := cell.ClThrottle(ctx, 1)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	fmt.Println(res)
	assert.Equal(t, res.Blocked, false)
	assert.Equal(t, res.Max, cell.opt.MaxBurst+1)
	time.Sleep(1 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}
