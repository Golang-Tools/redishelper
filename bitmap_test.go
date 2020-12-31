package redishelper

import (
	"context"
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
	ctx := context.Background()
	_, err = proxy.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	res, err := proxy.BitmapIsSetted(ctx, "testbitmap", 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	err = proxy.BitmapSet(ctx, "testbitmap", 12)
	if err != nil {
		assert.Error(t, err, "BitmapSet error")
	}
	res, err = proxy.BitmapIsSetted(ctx, "testbitmap", 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	offsetlist, err := proxy.BitmapSettedOffsets(ctx, "testbitmap")
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12}, offsetlist)

	err = proxy.BitmapUnSet(ctx, "testbitmap", 12)
	if err != nil {
		assert.Error(t, err, "BitmapSet error")
	}
	res, err = proxy.BitmapIsSetted(ctx, "testbitmap", 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	offsetlist, err = proxy.BitmapSettedOffsets(ctx, "testbitmap")
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{}, offsetlist)
}

func Test_bitmap_SetM(t *testing.T) {
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
	err = proxy.BitmapSetM(ctx, "testbitmapM", 12, 16, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err := proxy.BitmapSettedOffsets(ctx, "testbitmapM")
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 16, 1001}, offsetlist)

	err = proxy.BitmapSetM(ctx, "testbitmapM", 13, 1, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err = proxy.BitmapSettedOffsets(ctx, "testbitmapM")
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{1, 12, 13, 16, 1001}, offsetlist)

	err = proxy.BitmapUnSetM(ctx, "testbitmapM", 3, 1, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err = proxy.BitmapSettedOffsets(ctx, "testbitmapM")
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 13, 16}, offsetlist)
}
