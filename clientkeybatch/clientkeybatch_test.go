package clientkeybatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/exception"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

//TEST_REDIS_URL 测试用的无效redis地址
const TEST_REDIS_URL_NO_CONN = "redis://localhost:6378"

//准备工作
func NewBackground(t *testing.T, URL string) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	// _, err = cli.FlushDB(ctx).Result()
	// if err != nil {
	// 	assert.FailNow(t, err.Error(), "FlushDB error")
	// }
	fmt.Println("prepare task done")
	return cli, ctx
}

func Test_new_key_with_multi_opts(t *testing.T) {
	// 准备工作
	cli, _ := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	//开始
	_, err := New(cli, []string{"test_batchkey1", "test_batchkey1"}, &clientkey.Option{}, &clientkey.Option{})
	if err != nil {
		assert.Equal(t, exception.ErrParamOptsLengthMustLessThan2, err)
	} else {
		assert.FailNow(t, "key new get error")
	}
}

// func Test_new_key_no_conn(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL_NO_CONN)
// 	defer cli.Close()
// 	//开始
// 	key, err := New(cli, "test_key2", &Option{
// 		MaxTTL:              3 * time.Second,
// 		AutoRefreshInterval: "*/1 * * * *",
// 	})
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new get error")
// 	}
// 	_, err = key.Exists(ctx)
// 	assert.NotNil(t, err)
// 	// 测试type
// 	_, err = key.Type(ctx)
// 	assert.NotNil(t, err)
// 	// 测试TTL
// 	_, err = key.TTL(ctx)
// 	assert.NotNil(t, err)

// 	// 测试refreshTTL
// 	err = key.RefreshTTL(ctx)
// 	assert.NotNil(t, err)
// 	// 测试Delete
// 	err = key.Delete(ctx)
// 	assert.NotNil(t, err)
// }

// // 测试创建一个key并为其设置值
// func Test_new_key(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	//开始
// 	key, err := New(cli, "test_key2")
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new get error")
// 	}
// 	ok, err := key.Exists(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, false, ok)
// 	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Set key error")
// 	}
// 	ok, err = key.Exists(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, true, ok)
// 	// 测试type
// 	typename, err := key.Type(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, "string", typename)

// 	// 测试TTL
// 	left, err := key.TTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new TTL get error")
// 	}
// 	assert.Equal(t, time.Duration(-1), left)

// 	// 测试refreshTTL
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotSetMaxTLL, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 测试Delete
// 	err = key.Delete(ctx)
// 	if err != nil {
// 		assert.FailNow(t, "not get error")
// 	}
// }

// // 测试用nil作为opt参数创建一个key并不为其设置值
// func Test_new_key_with_empty_opt_and_not_exits_key(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	//开始
// 	key, err := New(cli, "test_key3", nil)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new get error")
// 	}
// 	ok, err := key.Exists(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, false, ok)
// 	// 测试 type
// 	_, err = key.Type(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 测试TTL
// 	_, err = key.TTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 测试Delete
// 	err = key.Delete(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 测试RefreshTTL
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotSetMaxTLL, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// }

// func Test_new_key_with_maxttl_and_ttl_op(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	// 开始
// 	key, err := New(cli, "test_key4", &Option{MaxTTL: 3 * time.Second})
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new get error")
// 	}
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Set key error")
// 	}
// 	ok, err := key.Exists(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, true, ok)
// 	// 还未设置过期
// 	left, err := key.TTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, time.Duration(-1), left)

// 	//设置过期
// 	err = key.RefreshTTL(ctx)
// 	time.Sleep(1 * time.Second)
// 	//测试ttl还剩少于2s
// 	left, err = key.TTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.LessOrEqual(t, int64(left), int64(2*time.Second))
// 	assert.LessOrEqual(t, int64(2*time.Second), int64(left))
// 	// 测试autorefresh报错
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskInterval, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	//未启动自动更新就停止
// 	err = key.StopAutoRefresh(false)
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	err = key.StopAutoRefresh(true)
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	time.Sleep(2 * time.Second)
// 	ok, err = key.Exists(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	assert.Equal(t, false, ok)
// }

// func Test_new_key_with_defaultautorefresh(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	// 开始
// 	key, err := New(cli, "test_key5", &Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 没有key刷新
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 设置key
// 	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 自动刷新
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	time.Sleep(100 * time.Second)
// 	left, err := key.TTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	fmt.Println("left", left)
// 	assert.LessOrEqual(t, int64(10*time.Second), int64(left))
// 	err = key.StopAutoRefresh(true)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// }

// func Test_new_key_with_defaultautorefresh_close_before_autorefresh(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	// 开始
// 	key, err := New(cli, "test_key5", &Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 没有key刷新
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 设置key
// 	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	//未启动自动更新就停止
// 	err = key.StopAutoRefresh(false)
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	err = key.StopAutoRefresh(true)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 自动刷新
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	time.Sleep(100 * time.Second)
// 	_, err = key.TTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}

// }

// func Test_new_key_with_defaultautorefresh_soft_close(t *testing.T) {
// 	// 准备工作
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	// 开始
// 	key, err := New(cli, "test_key6", &Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 没有key刷新
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.Equal(t, ErrKeyNotExist, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	// 设置key
// 	_, err = key.Client.Set(ctx, key.Key, "ok", 0).Result()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.RefreshTTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	// 自动刷新
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	err = key.AutoRefresh()
// 	if err != nil {
// 		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
// 	} else {
// 		assert.FailNow(t, "not get error")
// 	}
// 	time.Sleep(100 * time.Second)
// 	left, err := key.TTL(ctx)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// 	fmt.Println("left", left)
// 	assert.LessOrEqual(t, int64(10*time.Second), int64(left))
// 	err = key.StopAutoRefresh(false)
// 	if err != nil {
// 		assert.FailNow(t, err.Error(), "key new Exist get error")
// 	}
// }
