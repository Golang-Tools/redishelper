package redishelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_counter_counter(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}

	ctx := context.Background()
	_, err = proxy.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	res, err := proxy.Counter(ctx, "testcounter")
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)
	res, err = proxy.CounterM(ctx, "testcounter", 4)
	if err != nil {
		assert.Error(t, err, "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
	err = proxy.CounterReSet(ctx, "testcounter")
	if err != nil {
		assert.Error(t, err, "counter.reset error")
	}
	res, err = proxy.Counter(ctx, "testcounter")
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)

}
