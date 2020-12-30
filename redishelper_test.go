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

func Test_redisProxy_InitFromURL(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	ctx := context.Background()
	_, err = proxy.Set(ctx, "teststring", "ok", 10*time.Second).Result()
	if err != nil {
		assert.Error(t, err, "conn set error")
	}
	res, err := proxy.Get(ctx, "teststring").Result()
	if err != nil {
		assert.Error(t, err, "conn get error")
	}
	assert.Equal(t, "ok", res)
}

func Test_redisProxy_reset(t *testing.T) {
	proxy := New()
	proxy.Regist(func(cli GoRedisV8Client) error {
		t.Log("inited db")
		return nil
	})
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "reset from url error")
	}
	cli := redis.NewClient(options)
	err = proxy.SetConnect(cli)
	if err != nil {
		assert.Error(t, err, "conn reset error")
	}
}
