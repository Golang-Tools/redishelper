package ranker

import (
	"context"
	"testing"

	"github.com/Golang-Tools/redishelper/utils"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_ranker_FirstLast(t *testing.T) {
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
	ranker := New(cli, "testranker")

	//开始测试
	ranker.PushOrUpdate(ctx, &redis.Z{
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
	res, err := ranker.First(ctx, 2, true)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, []string{"d", "c"}, res)
	res, err = ranker.First(ctx, 2, false)
	if err != nil {
		assert.Error(t, err, "ranker First error")
	}
	assert.Equal(t, []string{"a", "b"}, res)

	res, err = ranker.Last(ctx, 2, true)
	if err != nil {
		assert.Error(t, err, "ranker Last desc error")
	}
	assert.Equal(t, []string{"a", "b"}, res)
	res, err = ranker.Last(ctx, 2, false)
	if err != nil {
		assert.Error(t, err, "ranker Last error")
	}
	assert.Equal(t, []string{"d", "c"}, res)
}

func Test_ranker_GetRank(t *testing.T) {
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
	ranker := New(cli, "testranker")

	//开始测试
	ranker.PushOrUpdate(ctx, &redis.Z{
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
	res, err = ranker.GetRank(ctx, "a", false)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(1), res)
	res, err = ranker.GetRank(ctx, "a", true)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(4), res)
	res, err = ranker.GetRank(ctx, "e", true)
	if err != nil {
		assert.Equal(t, utils.ErrElementNotExist, err)
	}
}
