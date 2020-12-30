package redishelper

import (
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

func Test_ranker_FirstLast(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	ranker := proxy.NewRanker("test_ranker")
	ranker.AddOrUpdate(&redis.Z{
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
	res, err := ranker.First(2, true)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, []string{"d", "c"}, res)
	res, err = ranker.First(2, false)
	if err != nil {
		assert.Error(t, err, "ranker First error")
	}
	assert.Equal(t, []string{"a", "b"}, res)

	res, err = ranker.Last(2, true)
	if err != nil {
		assert.Error(t, err, "ranker Last desc error")
	}
	assert.Equal(t, []string{"a", "b"}, res)
	res, err = ranker.Last(2, false)
	if err != nil {
		assert.Error(t, err, "ranker Last error")
	}
	assert.Equal(t, []string{"d", "c"}, res)

}

func Test_ranker_GetRank(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	ranker := proxy.NewRanker("test_ranker")
	ranker.AddOrUpdate(&redis.Z{
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
	res, err := ranker.Len()
	if err != nil {
		assert.Error(t, err, "ranker len error")
	}
	assert.Equal(t, int64(4), res)
	res, err = ranker.GetRank("a", false)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(1), res)
	res, err = ranker.GetRank("a", true)
	if err != nil {
		assert.Error(t, err, "ranker First desc error")
	}
	assert.Equal(t, int64(4), res)
	res, err = ranker.GetRank("e", true)
	if err != nil {
		//assert.Error(t, err, "ranker First desc error")
		assert.Equal(t, ErrElementNotExist, err)
	}
	//assert.Equal(t, int64(4), res)
}
