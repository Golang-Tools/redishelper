package topk

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

func Test_Topk_Incr_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk-ttl"
	bf, err := New(cli, WithSpecifiedKey(key), WithInit(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置,add
	_, err = bf.IncrItem(ctx, "a", IncrWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	time.Sleep(1 * time.Second)
	//二次设置,add
	_, err = bf.IncrItem(ctx, "a", IncrWithTTL(3*time.Second))
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

func Test_Topk_Incr_fttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk-fttl"
	bf, err := New(cli, WithSpecifiedKey(key), WithInit(3), WithMaxTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	time.Sleep(2 * time.Second)
	//一次设置,add
	_, err = bf.IncrItem(ctx, "a", IncrWithRefreshTTL())
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

func Test_Topk_MIncr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk"
	bf, err := New(cli, WithSpecifiedKey(key), WithInit(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	//一次设置,add
	res, err := bf.IncrItem(ctx, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	assert.Empty(t, res)
	//二次设置,incr
	res, err = bf.IncrItem(ctx, "b", IncrWithIncrement(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem with Increment error")
	}
	assert.Empty(t, res)
	//检查
	check, err := bf.QueryItem(ctx, "c")
	if err != nil {
		assert.FailNow(t, err.Error(), "QueryItem error")
	}
	assert.Equal(t, false, check)

	//3次设置,mincr
	res, err = bf.MIncrItem(ctx, IncrWithItemMap(map[string]int64{
		"a": 10,
		"c": 20,
		"d": 22,
	}))
	if err != nil {
		assert.FailNow(t, err.Error(), "MIncrItem  error")
	}
	assert.Contains(t, res, "b")
	cres, err := bf.CountItem(ctx, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	assert.GreaterOrEqual(t, int64(11), cres)
	lres, err := bf.List(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "List error")
	}
	assert.EqualValues(t, []string{"d", "c", "a"}, lres)

	lwcres, err := bf.ListWithCount(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "ListWithCount error")
	}
	resm := map[string]int64{
		"a": 11,
		"c": 20,
		"d": 22,
	}
	eleorder := []string{}
	for _, rr := range lwcres {
		eleorder = append(eleorder, rr.Item)
		assert.GreaterOrEqual(t, resm[rr.Item], rr.Count)
	}
	assert.EqualValues(t, []string{"d", "c", "a"}, eleorder)
	info, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(7), info.Depth)
	assert.Equal(t, int64(8), info.Width)
	bf.Clean(ctx)
	exists, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), exists)
}
