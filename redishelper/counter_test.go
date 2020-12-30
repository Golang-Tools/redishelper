package redishelper

import (
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
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	counter := proxy.NewCounter("testcounter")
	res, err := counter.Count()
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)
	res, err = counter.CountM(4)
	if err != nil {
		assert.Error(t, err, "counter.CountM error")
	}
	assert.Equal(t, int64(5), res)
	err = counter.ReSet()
	if err != nil {
		assert.Error(t, err, "counter.reset error")
	}
	res, err = counter.Count()
	if err != nil {
		assert.Error(t, err, "counter.Count error")
	}
	assert.Equal(t, int64(1), res)

}
