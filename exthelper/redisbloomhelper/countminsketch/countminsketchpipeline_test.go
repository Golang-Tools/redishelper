package countminsketch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CountMinSketchpipe_newdim_Incr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms-pipe"
	pipe := cli.Pipeline()

	//初始化
	_, err := InitPipe(pipe, ctx, key, InitWithDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "InitPipe error")
	}
	//一次设置
	res1, err := IncrItemPipe(pipe, ctx, key, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItemPipe1 error")
	}
	//二次设置
	res2, err := IncrItemPipe(pipe, ctx, key, "abc", IncrWithIncrement(3))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItemPipe2 error")
	}
	//检查
	res3, err := QueryItemPipe(pipe, ctx, key, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "QueryItemPipe error")
	}
	//检查状态
	res4 := InfoPipe(pipe, ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}

	res, err := res1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(1), res)

	res, err = res2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(4), res)

	res, err = res3.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(4), res)

	info, err := res4.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res4.Result error")
	}
	assert.Equal(t, int64(5), info.Depth)
	assert.Equal(t, int64(2000), info.Width)
}
func Test_CountMinSketchpipe_initprob_MIncr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cms-pipe-prob"
	pipe := cli.Pipeline()

	//初始化
	_, err := InitPipe(pipe, ctx, key, InitWithProbability(0.001, 0.01))
	if err != nil {
		assert.FailNow(t, err.Error(), "InitPipe error")
	}
	//一次设置
	res1, err := MIncrItemPipe(pipe, ctx, key, IncrWithItemMap(map[string]int64{"abc": 1, "cde": 2, "def": 3}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItemPipe1 error")
	}
	//检查
	res3, err := MQueryItemPipe(pipe, ctx, key, "abc", "cde", "def")
	if err != nil {
		assert.FailNow(t, err.Error(), "QueryItemPipe error")
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}

	res, err := res1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(1), res["abc"])
	assert.Equal(t, int64(2), res["cde"])
	assert.Equal(t, int64(3), res["def"])

	res, err = res3.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(1), res["abc"])
	assert.Equal(t, int64(2), res["cde"])
	assert.Equal(t, int64(3), res["def"])
}

func Test_CountMinSketchpipe_Merge(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()

	//资源1
	bfsource1, err := New(cli, WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	_, err = bfsource1.MIncrItem(ctx, IncrWithItemMap(map[string]int64{"abc": 1, "cde": 2, "def": 3}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}
	//资源2
	bfsource2, err := New(cli, WithInitDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCountMinSketch error")
	}
	_, err = bfsource2.MIncrItem(ctx, IncrWithItemMap(map[string]int64{"abc": 10, "cde": 20, "def": 30, "xyz": 123}))
	if err != nil {
		assert.FailNow(t, err.Error(), "IncrItem error")
	}

	sources := []*WeightedCountMinSketchKey{{Key: bfsource1.Key(), Weight: 1}, {Key: bfsource2.Key(), Weight: 1}}
	key := "test-cms-pipe-merge"
	pipe := cli.Pipeline()
	//初始化
	_, err = MergePipe(pipe, ctx, key, sources, InitWithDIM(2000, 5))
	if err != nil {
		assert.FailNow(t, err.Error(), "InitPipe error")
	}
	//检查
	res1, err := MQueryItemPipe(pipe, ctx, key, "abc", "cde", "def", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MQueryItemPipe error")
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	resnew, err := res1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "res1.Result error")
	}
	assert.Equal(t, int64(11), resnew["abc"])
	assert.Equal(t, int64(22), resnew["cde"])
	assert.Equal(t, int64(33), resnew["def"])
	assert.Equal(t, int64(123), resnew["xyz"])
}
