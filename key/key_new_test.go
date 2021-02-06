package key

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_new_key(t *testing.T) {
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
	//开始
	key, err := New(cli, "test_key")
	if err != nil {
		assert.Error(t, err, "key new get error")
	}
	ok, err := key.Exists(ctx)
	if err != nil {
		assert.Error(t, err, "key new Exist get error")
	}
	assert.Equal(t, false, ok)
	_, err = key.Set(ctx, key.Key, "ok", 0).Result()
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
func Test_new_key_with_empty_opt_and_not_exits_key(t *testing.T) {
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
	//开始
	key, err := New(cli, "test_key", nil)
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

// func Test_new_key_with_maxttl_and_ttl_op(t *testing.T) {
// 	// 准备工作
// 	options, err := redis.ParseURL(TEST_REDIS_URL)
// 	if err != nil {
// 		assert.Error(t, err, "init from url error")
// 	}
// 	cli := redis.NewClient(options)
// 	defer cli.Close()
// 	ctx := context.Background()
// 	_, err = cli.FlushDB(ctx).Result()
// 	if err != nil {
// 		assert.Error(t, err, "FlushDB error")
// 	}
// 	//开始
// 	key, err := New(cli, "test_key", &Option{MaxTTL: 3 * time.Second})
// 	if err != nil {
// 		assert.Error(t, err, "key new get error")
// 	}
// 	ok, err := key.Exists(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, false, ok)
// 	_, err = key.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.Error(t, err, "key new Set key error")
// 	}
// 	ok, err = key.Exists(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, true, ok)
// 	typename, err := key.Type(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, "string", typename)
// }

// func Test_new_key_with_defaultautorefresh(t *testing.T) {
// 	// 准备工作
// 	options, err := redis.ParseURL(TEST_REDIS_URL)
// 	if err != nil {
// 		assert.Error(t, err, "init from url error")
// 	}
// 	cli := redis.NewClient(options)
// 	defer cli.Close()
// 	ctx := context.Background()
// 	_, err = cli.FlushDB(ctx).Result()
// 	if err != nil {
// 		assert.Error(t, err, "FlushDB error")
// 	}
// 	//开始
// 	key, err := New(cli, "test_key")
// 	if err != nil {
// 		assert.Error(t, err, "key new get error")
// 	}
// 	ok, err := key.Exists(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, false, ok)
// 	_, err = key.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.Error(t, err, "key new Set key error")
// 	}
// 	ok, err = key.Exists(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, true, ok)
// 	typename, err := key.Type(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "key new Exist get error")
// 	}
// 	assert.Equal(t, "string", typename)
// }
