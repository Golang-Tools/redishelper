package redishelper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bitmap_Set(t *testing.T) {
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
	bm := proxy.NewBitmap("testbitmap")
	res, err := bm.IsSetted(12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	err = bm.Set(12)
	if err != nil {
		assert.Error(t, err, "Bitmap.Set error")
	}
	res, err = bm.IsSetted(12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	offsetlist, err := bm.SettedOffsets()
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12}, offsetlist)
}

func Test_bitmap_SetM(t *testing.T) {
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
	bm := proxy.NewBitmap("testbitmap")
	res, err := bm.IsSetted(12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)

	err = bm.SetM([]int64{12, 16, 1001})
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	res, err = bm.IsSetted(12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	offsetlist, err := bm.SettedOffsets()
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 16, 1001}, offsetlist)
}
