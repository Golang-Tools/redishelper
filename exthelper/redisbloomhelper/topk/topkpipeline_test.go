package topk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Topkpipe_Incr_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk-pipe-ttl"
	pipe := cli.Pipeline()
	//init
	InitPipe(pipe, ctx, key, 3)
	//add
	_, err := IncrItemPipe(pipe, ctx, key, "a", IncrWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	// 过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}
func Test_Topkpipe_Incr_fttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk-pipe-fttl"
	pipe := cli.Pipeline()
	//init
	InitPipe(pipe, ctx, key, 3, InitWithTTL(2*time.Second))
	//add
	_, err := IncrItemPipe(pipe, ctx, key, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	// 过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Topkpipe_MIncr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-topk-pipe-fttl"
	pipe := cli.Pipeline()
	//init
	InitPipe(pipe, ctx, key, 3)

	//一次设置,add
	res1, err := IncrItemPipe(pipe, ctx, key, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	//二次设置,incr
	res2, err := IncrItemPipe(pipe, ctx, key, "b", IncrWithIncrement(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem with Increment error")
	}
	//检查
	check1, err := QueryItemPipe(pipe, ctx, key, "c")
	if err != nil {
		assert.FailNow(t, err.Error(), "QueryItem error")
	}
	//3次设置,mincr
	res3, err := MIncrItemPipe(pipe, ctx, key, IncrWithItemMap(map[string]int64{
		"a": 10,
		"c": 20,
		"d": 22,
	}))
	if err != nil {
		assert.FailNow(t, err.Error(), "MIncrItem  error")
	}
	//count
	count1, err := CountItemPipe(pipe, ctx, key, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	//list
	list1 := ListPipe(pipe, ctx, key)
	//ListWithCount
	lwc := ListWithCountPipe(pipe, ctx, key)
	//info
	info1 := InfoPipe(pipe, ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	//res1
	res, err := res1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result() error")
	}
	assert.Empty(t, res)
	//res2
	res, err = res2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res2.Result() error")
	}
	assert.Empty(t, res)

	//check1
	check, err := check1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "check1.Result() error")
	}
	assert.Equal(t, false, check)
	//res3
	res, err = res3.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res3.Result() error")
	}
	assert.Contains(t, res, "b")
	//count1
	cres, err := count1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "count1.Result() error")
	}
	assert.GreaterOrEqual(t, int64(11), cres)
	//list
	lres, err := list1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "list1.Result() error")
	}
	assert.EqualValues(t, []string{"d", "c", "a"}, lres)
	//ListWithCount
	lwcres, err := lwc.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "lwc.Result() error")
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
	//info
	info, err := info1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "info1.Result() error")
	}
	assert.Equal(t, int64(7), info.Depth)
	assert.Equal(t, int64(8), info.Width)
}
