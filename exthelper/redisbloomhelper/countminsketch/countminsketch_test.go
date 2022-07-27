package countminsketch

import (
	"context"
	"fmt"
	"testing"
	"time"

	// log "github.com/Golang-Tools/loggerhelper/v2"
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
func Test_CountMinSketch_newdim_Incr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms"
	bf, err := New(cli, WithSpecifiedKey(key), WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置
	res, err := bf.IncrItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	assert.Equal(t, int64(1), res)
	//二次设置
	res, err = bf.IncrItem(ctx, "abc", IncrWithIncrement(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	assert.Equal(t, int64(4), res)
	//检查
	res, err = bf.QueryItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "QueryItem error")
	}
	assert.Equal(t, int64(4), res)
	info, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(5), info.Depth)
	assert.Equal(t, int64(2000), info.Width)
}
func Test_CountMinSketch_newdim_Incr_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms-ttl"
	bf, err := New(cli, WithSpecifiedKey(key), WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置
	_, err = bf.IncrItem(ctx, "abc", IncrWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	time.Sleep(1 * time.Second)
	//二次设置,刷新过期
	_, err = bf.IncrItem(ctx, "abc", IncrWithTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	//未过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(1), c)
	// 过期
	time.Sleep(2 * time.Second)
	c, err = cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_CountMinSketch_newdim_Incr_fttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms-fttl"
	bf, err := New(cli, WithSpecifiedKey(key), WithInitDIM(2000, 5), WithMaxTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	time.Sleep(1 * time.Second)
	//一次设置
	_, err = bf.IncrItem(ctx, "abc", IncrWithRefreshTTL())
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	//未过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(1), c)
	//过期
	time.Sleep(2 * time.Second)
	c, err = cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_CountMinSketch_initprob_MIncr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms-prob"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	bf.Init(ctx, InitWithProbability(0.001, 0.01))
	//一次设置
	res, err := bf.MIncrItem(ctx, IncrWithItemMap(map[string]int64{"abc": 1, "cde": 2, "def": 3}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	assert.Equal(t, int64(1), res["abc"])
	assert.Equal(t, int64(2), res["cde"])
	assert.Equal(t, int64(3), res["def"])
	res, err = bf.MQueryItem(ctx, "abc", "cde", "def")
	if err != nil {
		assert.FailNow(t, err.Error(), "MQueryItem error")
	}
	assert.Equal(t, int64(1), res["abc"])
	assert.Equal(t, int64(2), res["cde"])
	assert.Equal(t, int64(3), res["def"])
	bf.Clean(ctx)
	exists, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), exists)
}

func Test_CountMinSketch_Merge(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	//source
	key1 := "test-cms-mergesource"
	bfsource1, err := New(cli, WithSpecifiedKey(key1), WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置
	_, err = bfsource1.MIncrItem(ctx, IncrWithItemMap(map[string]int64{"abc": 1, "cde": 2, "def": 3}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	//self
	key2 := "test-cms-mergeself"
	bfself, err := New(cli, WithSpecifiedKey(key2), WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置
	_, err = bfself.MIncrItem(ctx, IncrWithItemMap(map[string]int64{"abc": 10, "cde": 20, "def": 30, "xyz": 123}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	newcms, err := bfself.Merge(ctx, 1, []*WeightedCountMinSketch{{Src: bfsource1, Weight: 1}})
	if err != nil {
		assert.FailNow(t, err.Error(), "Merge error")
	}
	resnew, err := newcms.MQueryItem(ctx, "abc", "cde", "def", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, int64(11), resnew["abc"])
	assert.Equal(t, int64(22), resnew["cde"])
	assert.Equal(t, int64(33), resnew["def"])
	assert.Equal(t, int64(123), resnew["xyz"])
}
