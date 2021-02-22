package clientkeybatch

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	_, err := New(cli, []string{"test_batchkey1", "test_batchkey2"}, &clientkey.Option{}, &clientkey.Option{})
	if err != nil {
		assert.Equal(t, exception.ErrParamOptsLengthMustLessThan2, err)
	} else {
		assert.FailNow(t, "key new get error")
	}
}

func Test_new_key_no_conn(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL_NO_CONN)
	defer cli.Close()
	//开始
	key, err := New(cli, []string{"test_batchkey41", "test_batchkey42"}, &clientkey.Option{
		MaxTTL:              3 * time.Second,
		AutoRefreshInterval: "*/1 * * * *",
	})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new get error")
	}
	_, err = key.AllExists(ctx)
	assert.NotNil(t, err)
	_, err = key.AnyExists(ctx)
	assert.NotNil(t, err)
	// 测试type
	_, err = key.Types(ctx)
	assert.NotNil(t, err)
	// 测试TTL
	_, err = key.TTL(ctx)
	assert.NotNil(t, err)

	// 测试refreshTTL
	err = key.RefreshTTL(ctx)
	assert.NotNil(t, err)
	// 测试Delete
	err = key.Delete(ctx)
	assert.NotNil(t, err)
}

// 测试创建一个key并为其设置值
func Test_new_key(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	//开始
	key, err := New(cli, []string{"test_batchkey31", "test_batchkey32"})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new get error")
	}
	ok, err := key.AnyExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, false, ok)
	_, err = key.Client.Set(ctx, key.Keys[0], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Set key error")
	}
	ok, err = key.AnyExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	_, err = key.Client.Set(ctx, key.Keys[1], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Set key error")
	}
	assert.Equal(t, true, ok)
	ok, err = key.AllExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, true, ok)
	// 测试type
	typenames, err := key.Types(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, "string", typenames[key.Keys[1]])

	// 测试TTL
	left, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new TTL get error")
	}
	assert.Equal(t, time.Duration(-1), left[key.Keys[1]])

	// 测试refreshTTL
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.Equal(t, ErrBatchNotSetMaxTLL, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	// 测试Delete
	err = key.Delete(ctx)
	if err != nil {
		assert.FailNow(t, "not get error")
	}
}

// 测试用nil作为opt参数创建一个key并不为其设置值
func Test_new_key_with_empty_opt_and_not_exits_key(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	//开始
	key, err := New(cli, []string{"test_batchkey21", "test_batchkey22"}, nil)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new get error")
	}
	ok, err := key.AnyExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, false, ok)
	// 测试 type
	typs, err := key.Types(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Types get error")
	}
	assert.Equal(t, "", typs[key.Keys[0]])
	// 测试TTL
	ttls, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new TTL get error")
	}
	assert.Equal(t, time.Duration(-2), ttls[key.Keys[0]])
	// 测试RefreshTTL
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.Equal(t, ErrBatchNotSetMaxTLL, err)
	} else {
		assert.FailNow(t, "not get error")
	}
}

func Test_new_key_with_maxttl_and_ttl_op(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	// 开始
	key, err := New(cli,
		[]string{"test_batchkey11", "test_batchkey12"},
		&clientkey.Option{MaxTTL: 3 * time.Second})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new get error")
	}
	_, err = key.Client.Set(ctx, key.Keys[0], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Set key error")
	}
	ok, err := key.AnyExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, true, ok)

	_, err = key.Client.Set(ctx, key.Keys[1], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Set key error")
	}
	ok, err = key.AllExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, true, ok)

	// 还未设置过期
	left, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, time.Duration(-1), left[key.Keys[0]])

	//设置过期
	err = key.RefreshTTL(ctx)
	time.Sleep(1 * time.Second)
	//测试ttl还剩少于2s
	left, err = key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.LessOrEqual(t, int64(left[key.Keys[0]]), int64(2*time.Second))
	assert.LessOrEqual(t, int64(2*time.Second), int64(left[key.Keys[0]]))
	// 测试autorefresh报错
	err = key.AutoRefresh()
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskInterval, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	//未启动自动更新就停止
	err = key.StopAutoRefresh(false)
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	err = key.StopAutoRefresh(true)
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	time.Sleep(2 * time.Second)
	ok, err = key.AnyExists(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	assert.Equal(t, false, ok)
}

func Test_new_key_with_defaultautorefresh(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	// 开始
	key, err := New(cli,
		[]string{"test_batchkey61", "test_batchkey62"},
		&clientkey.Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[0], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[1], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 自动刷新
	err = key.AutoRefresh()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.AutoRefresh()
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	time.Sleep(100 * time.Second)
	left, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	fmt.Println("left", left)
	assert.LessOrEqual(t, int64(10*time.Second), int64(left[key.Keys[0]]))
	err = key.StopAutoRefresh(true)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
}

func Test_new_key_with_defaultautorefresh_close_before_autorefresh(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	// 开始
	key, err := New(cli,
		[]string{"test_batchkey71", "test_batchkey72"},
		&clientkey.Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[0], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[1], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	//未启动自动更新就停止
	err = key.StopAutoRefresh(false)
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHNotSetYet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	err = key.StopAutoRefresh(true)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 自动刷新
	err = key.AutoRefresh()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.AutoRefresh()
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	time.Sleep(100 * time.Second)
	ttls, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")

	}
	assert.Equal(t, time.Duration(-2), ttls[key.Keys[0]])
}

func Test_new_key_with_defaultautorefresh_soft_close(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	// 开始
	key, err := New(cli,
		[]string{"test_batchkey81", "test_batchkey82"},
		&clientkey.Option{MaxTTL: 100 * time.Second, AutoRefreshInterval: "*/1 * * * *"})
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[0], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 设置key
	_, err = key.Client.Set(ctx, key.Keys[1], "ok", 0).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.RefreshTTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	// 自动刷新
	err = key.AutoRefresh()
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	err = key.AutoRefresh()
	if err != nil {
		assert.Equal(t, ErrAutoRefreshTaskHasBeenSet, err)
	} else {
		assert.FailNow(t, "not get error")
	}
	time.Sleep(100 * time.Second)
	left, err := key.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
	fmt.Println("left", left)
	assert.LessOrEqual(t, int64(10*time.Second), int64(left[key.Keys[0]]))
	err = key.StopAutoRefresh(false)
	if err != nil {
		assert.FailNow(t, err.Error(), "key new Exist get error")
	}
}
