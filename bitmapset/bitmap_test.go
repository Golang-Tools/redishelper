package bitmapset

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	redis "github.com/go-redis/redis/v8"
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

func Test_bitmap_without_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bitmapset"
	bm, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}

	res, err := bm.Contained(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	err = bm.Add(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	res, err = bm.Contained(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	count, err := bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(1), count)
	offsetlist, err := bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12}, offsetlist)
	err = bm.AddM(ctx, 12, 13, 14, 16)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	count, err = bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(4), count)
	// 测试ScopCount
	count, err = bm.ScopCount(ctx, ScopIndex(2))
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(1), count)

	count, err = bm.ScopCount(ctx, ScopRange(1, 1))
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(3), count)

	offsetlist, err = bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 13, 14, 16}, offsetlist)

	err = bm.Remove(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	res, err = bm.Contained(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	count, err = bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(3), count)
	offsetlist, err = bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{13, 14, 16}, offsetlist)
	err = bm.Remove(ctx, 10)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	count, err = bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(3), count)
	offsetlist, err = bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{13, 14, 16}, offsetlist)

	err = bm.RemoveM(ctx, 14, 16)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	count, err = bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(1), count)
	offsetlist, err = bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{13}, offsetlist)
	err = bm.Reset(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.SettedOffsets error")
	}
	err = bm.Reset(ctx)
	assert.NotNil(t, err)
}

func Test_bitmap_op(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()

	bm1, err := New(cli, WithSpecifiedKey("test_bitmap1"))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	bm2, err := New(cli, WithSpecifiedKey("test_bitmap2"))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}

	err = bm1.AddM(ctx, 10, 12, 13, 14, 16)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	err = bm2.AddM(ctx, 11, 12, 13, 14, 15)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	//测试交集
	bmi, err := bm1.Intersection(ctx, []*Bitmap{bm2})
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err := bmi.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{12, 13, 14}, offsetlist)
	//测试并集
	bmu, err := bm1.Union(ctx, []*Bitmap{bm2})
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bmu.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 11, 12, 13, 14, 15, 16}, offsetlist)
	//测试对称差集
	bmx, err := bm1.Xor(ctx, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bmx.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 11, 15, 16}, offsetlist)

	//测试差集
	bme, err := bm1.Except(ctx, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bme.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 16}, offsetlist)
}

func Test_bitmap_TTL(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()

	bm, err := New(cli, WithSpecifiedKey("test_bitmap"), WithMaxTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	//开始测试
	err = bm.Add(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	//contained会刷新
	res, err := bm.Contained(ctx, 12)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, true, res)
	ttl, err := bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(1*time.Second))
	assert.LessOrEqual(t, int64(0*time.Second), int64(ttl))
	//ToArray会刷新
	offsetlist, err := bm.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{12}, offsetlist)
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)
	//addM会刷新
	err = bm.AddM(ctx, 13, 15, 17)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)

	//Remove会刷新
	err = bm.Remove(ctx, 13)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)

	//Remove会刷新
	err = bm.RemoveM(ctx, 12, 17)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)

	//Len会刷新
	_, err = bm.Len(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.LessOrEqual(t, int64(ttl), int64(2*time.Second))
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	//结束
	time.Sleep(3 * time.Second)
	_, err = bm.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, middlewarehelper.ErrKeyNotExists)
	} else {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
}
