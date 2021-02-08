package bitmap

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, keyname string, opt *clientkey.Option) (*clientkey.ClientKey, context.Context) {
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
	key, err := clientkey.New(cli, keyname, opt)
	if err != nil {
		assert.FailNow(t, err.Error(), "create key error")
	}
	fmt.Println("prepare task done")
	return key, ctx
}

func Test_bitmap_without_ttl(t *testing.T) {
	key, ctx := NewBackground(t, "test_bitmap", nil)
	defer key.Client.Close()
	//开始测试
	bm := New(key)
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
	count, err = bm.ScopCount(ctx, 2)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(1), count)

	count, err = bm.ScopCount(ctx, 1, 1)
	if err != nil {
		assert.FailNow(t, err.Error(), "Bitmap.IsSetted error")
	}
	assert.Equal(t, int64(3), count)

	_, err = bm.ScopCount(ctx, 1, 1, 1)
	if err != nil {
		assert.Equal(t, ErrParamScopLengthMoreThan2, err)
	} else {
		assert.FailNow(t, "no get error")
	}

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
}

func Test_bitmap_op(t *testing.T) {
	key1, ctx := NewBackground(t, "test_bitmap1", nil)
	key2, ctx := NewBackground(t, "test_bitmap2", nil)
	key3, ctx := NewBackground(t, "test_bitmap3", nil)
	defer key1.Client.Close()
	//开始测试
	bm1 := New(key1)
	bm2 := New(key2)
	err := bm1.AddM(ctx, 10, 12, 13, 14, 16)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	err = bm2.AddM(ctx, 11, 12, 13, 14, 15)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	//测试交集
	bm3, err := bm1.Intersection(ctx, key3, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err := bm3.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{12, 13, 14}, offsetlist)
	//测试并集
	bm3, err = bm1.Union(ctx, key3, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bm3.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 11, 12, 13, 14, 15, 16}, offsetlist)
	//测试对称差集
	bm3, err = bm1.Xor(ctx, key3, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bm3.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 11, 15, 16}, offsetlist)

	//测试差集
	bm3, err = bm1.Except(ctx, key3, bm2)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	offsetlist, err = bm3.ToArray(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
	assert.Equal(t, []int64{10, 16}, offsetlist)
}

func Test_bitmap_TTL(t *testing.T) {
	key, ctx := NewBackground(t, "test_bitmap", &clientkey.Option{
		MaxTTL: 2 * time.Second,
	})
	defer key.Client.Close()

	bm := New(key)

	//开始测试
	err := bm.Add(ctx, 12)
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
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, clientkey.ErrKeyNotExist)
	} else {
		assert.FailNow(t, err.Error(), "BitmapSet error")
	}
}
