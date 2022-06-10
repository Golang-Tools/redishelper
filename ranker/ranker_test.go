package ranker

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackgroundClient(t *testing.T) (redis.UniversalClient, context.Context) {
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
	return cli, ctx
}

func Test_ranker_FirstLast(t *testing.T) {
	// 准备工作
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()

	ranker, err := New(cli, WithSpecifiedKey("test_ranker"))
	if err != nil {
		assert.FailNow(t, err.Error(), "New get error")
	}
	//开始测试
	ranker.AddOrUpdateM(ctx, &redis.Z{
		Member: "a",
		Score:  1.0,
	}, &redis.Z{
		Member: "b",
		Score:  1.1,
	}, &redis.Z{
		Member: "c",
		Score:  1.2,
	}, &redis.Z{
		Member: "d",
		Score:  1.3,
	})
	res, err := ranker.First(ctx, 2, Reverse())
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, []string{"d", "c"}, res)
	res, err = ranker.First(ctx, 2)
	if err != nil {
		assert.Error(t, err, "ranker First error")
	}
	assert.Equal(t, []string{"a", "b"}, res)

	res, err = ranker.Last(ctx, 2, Reverse())
	if err != nil {
		assert.Error(t, err, "ranker Last desc error")
	}
	assert.Equal(t, []string{"a", "b"}, res)
	res, err = ranker.Last(ctx, 2)
	if err != nil {
		assert.Error(t, err, "ranker Last error")
	}
	assert.Equal(t, []string{"d", "c"}, res)
}

func Test_ranker_GetRank(t *testing.T) {
	// 准备工作
	// 准备工作
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()

	ranker, err := New(cli, WithSpecifiedKey("test_ranker"))
	if err != nil {
		assert.FailNow(t, err.Error(), "New get error")
	}
	//开始测试

	ranker.AddOrUpdateM(ctx, &redis.Z{
		Member: "a",
		Score:  1.0,
	}, &redis.Z{
		Member: "b",
		Score:  1.1,
	}, &redis.Z{
		Member: "c",
		Score:  1.2,
	}, &redis.Z{
		Member: "d",
		Score:  1.3,
	})
	res, err := ranker.Len(ctx)
	if err != nil {
		assert.Error(t, err, "ranker len error")
	}
	assert.Equal(t, int64(4), res)
	res, err = ranker.GetRank(ctx, "a")
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(1), res)
	res, err = ranker.GetRank(ctx, "a", Reverse())
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(4), res)
	_, err = ranker.GetRank(ctx, "e", Reverse())
	if err != nil {
		assert.Equal(t, ErrElementNotExist, err)
	}
}
