package key

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/utils"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

//准备工作
func NewBackground(t *testing.T) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	fmt.Println("prepare task done")
	return cli, ctx
}
func Test_new_key_with_multi_opts(t *testing.T) {
	// 准备工作
	cli, _ := NewBackground(t)
	defer cli.Close()
	//开始
	_, err := New(cli, "test_key1", &Option{}, &Option{})
	if err != nil {
		assert.Equal(t, utils.ErrParamOptsLengthMustLessThan2, err)
	}
	assert.Error(t, err, "key new get error")
}

// 测试创建一个key并为其设置值
func Test_new_key(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t)
	defer cli.Close()
	//开始
	key, err := New(cli, "test_key2")
	if err != nil {
		assert.Error(t, err, "key new get error")
	}
	ok, err := key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, false, ok)
	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
	if err != nil {
		assert.Error(t, err, "key new Set key error")
	}
	ok, err = key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, true, ok)
	// 测试type
	typename, err := key.Type(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, "string", typename)

	// 测试TTL
	left, err := key.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "key new TTL get error")
	}
	assert.Equal(t, time.Duration(-1), left)

	// 测试Delete
	ok, err = key.Delete(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)
	}
	assert.Error(t, errors.New("not get error"))
}

// 测试用nil作为opt参数创建一个key并不为其设置值
func Test_new_key_with_empty_opt_and_not_exits_key(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t)
	defer cli.Close()
	//开始
	key, err := New(cli, "test_key3", nil)
	if err != nil {
		assert.Error(t, err, "key new get error")
	}
	ok, err := key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, false, ok)
	// 测试 type
	_, err = key.Type(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)

	}
	assert.Error(t, errors.New("not get error"))

	// 测试TTL
	_, err = key.TTL(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)
	}
	assert.Error(t, errors.New("not get error"))

	// 测试Delete
	_, err = key.Delete(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)

	}
	assert.Error(t, errors.New("not get error"))

	// 测试RefreshTTL
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)

	}
	assert.Error(t, errors.New("not get error"))

}

func Test_new_key_with_maxttl_and_ttl_op(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t)
	defer cli.Close()
	// 开始
	key, err := New(cli, "test_key4", &Option{MaxTTL: 3 * time.Second})
	if err != nil {
		assert.Error(t, err, "key new get error")
	}
	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
	if err != nil {
		assert.Error(t, err, "key new Set key error")
	}
	ok, err := key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, true, ok)
	// 还未设置过期
	left, err := key.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, time.Duration(-1), left)

	//设置过期
	err = key.RefreshTTL(ctx)
	time.Sleep(1 * time.Second)
	//测试ttl还剩少于2s
	left, err = key.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.LessOrEqual(t, int64(left), int64(2*time.Second))
	assert.LessOrEqual(t, int64(2*time.Second), int64(left))
	time.Sleep(2 * time.Second)
	ok, err = key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, false, ok)
}

func Test_new_key_with_defaultautorefresh(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t)
	defer cli.Close()
	// 开始
	key, err := New(cli, "test_key5", &Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	// 没有key刷新
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.Equal(t, ErrKeyNotExist, err)

	}
	assert.Error(t, errors.New("not get error"))
	// 设置key
	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	// 自动刷新
	err = key.AutoRefresh()
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	time.Sleep(100 * time.Second)
	left, err := key.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	fmt.Println("left", left)
	assert.LessOrEqual(t, int64(10*time.Second), int64(left))
	err = key.StopAutoRefresh(true)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
}
