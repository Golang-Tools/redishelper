package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

//TEST_REDIS_WRONG_URL 错误的redis url
const TEST_REDIS_WRONG_URL = "mysql://localhost:6379"

func Test_redisProxy_InitFromWrongURL(t *testing.T) {
	proxy := New()
	err := proxy.Init(WithURL(TEST_REDIS_WRONG_URL))
	assert.NotNil(t, err)
}

func Test_redisProxy_InitFromURL(t *testing.T) {
	//准备
	proxy := New()
	err := proxy.Init(WithURL(TEST_REDIS_URL))
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	defer proxy.Close()
	ctx := context.Background()
	_, err = proxy.FlushDB(ctx).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "FlushDB error")
	}

	// 测试
	_, err = proxy.Set(ctx, "teststring", "ok", 10*time.Second).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "conn set error")
	}
	res, err := proxy.Get(ctx, "teststring").Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "conn get error")
	}
	assert.Equal(t, "ok", res)
}

func Test_redisProxy_reset(t *testing.T) {
	proxy := New()
	proxy.Regist(func(cli redis.UniversalClient) error {
		t.Log("inited db")
		return nil
	})
	err := proxy.Init(WithURL(TEST_REDIS_URL))
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	defer proxy.Close()
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "reset from url error")
	}
	cli := redis.NewClient(options)
	err = proxy.SetConnect(cli)
	if err != nil {
		assert.Equal(t, ErrProxyAllreadySettedUniversalClient, err)
	} else {
		assert.FailNow(t, "not get error")
	}
}

func Test_redisProxy_InitFromURL_with_cb(t *testing.T) {
	proxy := New()
	// 测试cb顺序执行
	proxy.Regist(func(cli redis.UniversalClient) error {
		ctx := context.Background()
		_, err := cli.Set(ctx, "a", "t", 0).Result()
		if err != nil {
			return err
		}
		return nil
	})
	proxy.Regist(func(cli redis.UniversalClient) error {
		ctx := context.Background()
		res, err := cli.Get(ctx, "a").Result()
		if err != nil {
			return err
		}
		assert.Equal(t, "t", res)
		return nil
	})
	err := proxy.Init(WithURL(TEST_REDIS_URL))
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	defer proxy.Close()
}

func Test_redisProxy_regist_cb_after_InitFromURL(t *testing.T) {
	proxy := New()
	err := proxy.Init(WithURL(TEST_REDIS_URL))
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	defer proxy.Close()
	err = proxy.Regist(func(cli redis.UniversalClient) error {
		ctx := context.Background()
		_, err := cli.Set(ctx, "a", "t", 0).Result()
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		assert.Equal(t, ErrProxyAllreadySettedUniversalClient, err)
	} else {
		assert.FailNow(t, "not get error")
	}
}

func Test_redisProxy_InitFromURL_with_parallel_cb(t *testing.T) {
	proxy := New()
	// 测试cb顺序执行
	proxy.Regist(func(cli redis.UniversalClient) error {
		time.Sleep(3 * time.Second)
		ctx := context.Background()
		res, err := cli.Get(ctx, "a").Result()
		if err != nil {
			return err
		}
		assert.Equal(t, "e", res)
		_, err = cli.Set(ctx, "a", "t", 0).Result()
		if err != nil {
			return err
		}
		return nil
	})
	proxy.Regist(func(cli redis.UniversalClient) error {
		time.Sleep(2 * time.Second)
		ctx := context.Background()
		res, err := cli.Get(ctx, "a").Result()
		if err != nil {
			return err
		}
		assert.Equal(t, "s", res)
		_, err = cli.Set(ctx, "a", "e", 0).Result()
		if err != nil {
			return err
		}

		return nil
	})
	proxy.Regist(func(cli redis.UniversalClient) error {
		time.Sleep(1 * time.Second)
		ctx := context.Background()
		_, err := cli.Set(ctx, "a", "s", 0).Result()
		if err != nil {
			return err
		}
		return nil
	})
	err := proxy.Init(WithURL(TEST_REDIS_URL), WithParallelCallback())
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	defer proxy.Close()
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	res, err := proxy.Get(ctx, "a").Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	assert.Equal(t, "t", res)
}
