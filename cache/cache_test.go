package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/lock"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, cachekeyname string, cacheopt *clientkey.Option, lockkeyname string, lockopt *clientkey.Option) (*clientkey.ClientKey, *clientkey.ClientKey, context.Context) {
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
	lockkey, err := clientkey.New(cli, lockkeyname, lockopt)
	if err != nil {
		assert.FailNow(t, err.Error(), "create key error")
	}
	fmt.Println("prepare task done")
	cachekey, err := clientkey.New(cli, cachekeyname, cacheopt)
	if err != nil {
		assert.FailNow(t, err.Error(), "create key error")
	}
	fmt.Println("prepare task done")
	return lockkey, cachekey, ctx
}
func Test_new_Lock_err(t *testing.T) {
	// 准备工作
	lockkey, cachekey, ctx := NewBackground(t,
		"test_cache",
		nil,
		"test_cache_lock",
		&clientkey.Option{
			MaxTTL: 10 * time.Second,
		},
	)
	lock, err := lock.New(lockkey, "lock1")
	cache, err := New(cachekey, lock)
	if err != nil {
		assert.FailNow(t, err.Error(), "new cache error")
	}
	cache.RegistUpdateFunc(func() ([]byte, error) {
		a, err := randomkey.Next()
		if err != nil {
			return nil, err
		}
		return []byte(a), err
	})

	a, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get a error")
	}
	time.Sleep(1 * time.Second)
	b, err := cache.Get(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "cache.Get b error")
	}
	assert.Equal(t, a, b)
}
