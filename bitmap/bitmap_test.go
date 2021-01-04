package bitmap

import (
	"context"
	"errors"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_bitmap_TTL(t *testing.T) {
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
	bm := New(cli, "testbitmap", 2*time.Second)

	//开始测试
	err = bm.Set(ctx, 12)
	if err != nil {
		assert.Error(t, err, "BitmapSet error")
	}
	res, err := bm.IsSetted(ctx, 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	ttl, err := bm.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	log.Info("set ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(2*time.Second), int64(ttl))
	time.Sleep(1 * time.Second)
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	log.Info("get ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(1*time.Second), int64(ttl))
	offsetlist, err := bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12}, offsetlist)
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	log.Info("get ttl", log.Dict{"ttl": ttl})
	assert.LessOrEqual(t, int64(2*time.Second), int64(ttl))
	time.Sleep(3 * time.Second)
	ttl, err = bm.TTL(ctx)
	if err != nil {
		assert.Equal(t, err, ErrKeyNotExist)
	} else {
		assert.Error(t, errors.New("Bitmap.IsSetted error"))
	}
}

func Test_bitmap_NoKey(t *testing.T) {
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
	bm := New(cli, "testbitmap")

	//开始测试
	res, err := bm.IsSetted(ctx, 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	offsetlist, err := bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{}, offsetlist)
}

func Test_bitmap_Set(t *testing.T) {
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
	bm := New(cli, "testbitmap")

	//开始测试
	res, err := bm.IsSetted(ctx, 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	err = bm.Set(ctx, 12)
	if err != nil {
		assert.Error(t, err, "BitmapSet error")
	}
	res, err = bm.IsSetted(ctx, 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, true, res)
	offsetlist, err := bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12}, offsetlist)

	err = bm.UnSet(ctx, 12)
	if err != nil {
		assert.Error(t, err, "BitmapSet error")
	}
	res, err = bm.IsSetted(ctx, 12)
	if err != nil {
		assert.Error(t, err, "Bitmap.IsSetted error")
	}
	assert.Equal(t, false, res)
	offsetlist, err = bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{}, offsetlist)
}

func Test_bitmap_SetM(t *testing.T) {
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
	bm := New(cli, "testbitmapM")

	//开始测试
	err = bm.SetM(ctx, 12, 16, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err := bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 16, 1001}, offsetlist)

	err = bm.SetM(ctx, 13, 1, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err = bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{1, 12, 13, 16, 1001}, offsetlist)

	err = bm.UnSetM(ctx, 3, 1, 1001)
	if err != nil {
		assert.Error(t, err, "Bitmap.SetM error")
	}
	offsetlist, err = bm.SettedOffsets(ctx)
	if err != nil {
		assert.Error(t, err, "Bitmap.SettedOffsets error")
	}
	assert.Equal(t, []int64{12, 13, 16}, offsetlist)
}
