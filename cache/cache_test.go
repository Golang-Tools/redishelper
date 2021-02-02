package cache

import (
	"context"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/lock"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_new_Lock_err(t *testing.T) {
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
	lock1, err := lock.New(cli, "testlock", "client01", 10*time.Second)
	if err != nil {
		assert.Error(t, err, "new locked error")
	}
	cache, err := New("test_cache_key", 60*time.Second, lock1, cli)
	if err != nil {
		assert.Error(t, err, "new cache error")
	}
	cache.RegistUpdateFunc(func() ([]byte, error) {
		err := randomkey.InitGenerator(uint16(3))
		if err != nil {
			return nil, err
		}
		a, err := randomkey.NextKey()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx)
	if err != nil {
		assert.Error(t, err, "cache.Get a error")
	}
	time.Sleep(1)
	b, err := cache.Get(ctx)
	if err != nil {
		assert.Error(t, err, "cache.Get b error")
	}
	assert.Equal(t, a, b)
}
