package cuckoofilter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
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
func Test_Cuckoofilter_ADD(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//一次设置
	res, err := bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//二次设置
	res, err = bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//3次设置
	res, err = bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//不存在的
	res, err = bf.ExistsItem(ctx, "abcd")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, false, res)
}
func Test_Cuckoofilter_ADD_NX(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-nx"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//一次设置
	res, err := bf.AddItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//二次设置
	res, err = bf.AddItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, true, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//3次设置
	res, err = bf.AddItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, true, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//不存在的
	res, err = bf.ExistsItem(ctx, "abcd")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, false, res)
}

func Test_Cuckoofilter_ADD_With_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	res, err := bf.AddItem(ctx, "abc", AddWithTTL(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//刷新
	time.Sleep(1 * time.Second)
	res, err = bf.AddItem(ctx, "abcd", AddWithTTL(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	//未过期
	time.Sleep(1 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(1), c)
	// 过期
	time.Sleep(2 * time.Second)
	c, err = cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_ADD_With_ttl_firsttime(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-first"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	res, err := bf.AddItem(ctx, "abc", AddWithTTLAtFirstTime(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	res, err = bf.ExistsItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, true, res)
	//刷新(不会刷新)
	time.Sleep(1 * time.Second)
	res, err = bf.AddItem(ctx, "abcd", AddWithTTLAtFirstTime(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	//过期
	time.Sleep(1 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_ADDM(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-m"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//第一次
	res, err := bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])

	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	//第二次
	res, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err = bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(6), resinfo.NumberOfItemsInserted)
}

func Test_Cuckoofilter_ADDM_NX(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-m-nx"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//第一次
	res, err := bf.MAddItem(ctx, []string{"abc", "cde", "def"})
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])

	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	//第二次
	res, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"})
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, true, res["def"])
	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err = bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
}

func Test_Cuckoofilter_ADDM_withttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-m-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//第一次
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTL(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	//刷新
	time.Sleep(1 * time.Second)
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTL(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//未过期
	time.Sleep(1 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(1), c)
	// 过期
	time.Sleep(2 * time.Second)
	c, err = cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_ADDM_withttl_firsttime(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-m-ttl-first"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTLAtFirstTime(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	//刷新(不会刷新)
	time.Sleep(1 * time.Second)
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTLAtFirstTime(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	// 过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}
func Test_Cuckoofilter_Reserve_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-ri"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	err = bf.Reserve(ctx, 1000)
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(1024), resinfo.BucketSize*resinfo.NumberOfBuckets)
}

func Test_Cuckoofilter_Reserve_ttl_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-ri-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	err = bf.Reserve(ctx, 1000, ReserveWithBucketSize(4), ReserveWithMaxIterations(20), ReserveWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(1024), resinfo.BucketSize*resinfo.NumberOfBuckets)

	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_Reserve_mtl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-r-mtl"
	bf, err := New(cli, WithSpecifiedKey(key), WithMaxTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	err = bf.Reserve(ctx, 1000, ReserveWithRefreshTTL())
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	//刷新(不会刷新)
	time.Sleep(1 * time.Second)
	_, err = bf.AddItem(ctx, "abcd", AddWithTTLAtFirstTime(2*time.Second), AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-i"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	assert.EqualError(t, err, "ERR not found")

	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithCapacity(1000))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	res, err = bf.MExistsItem(ctx, "abc", "cde", "xyz")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["xyz"])
	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(1024), resinfo.BucketSize*resinfo.NumberOfBuckets)
	//删除key
	bf.Clean(ctx)
	exists, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), exists)
}

func Test_Cuckoofilter_Insert_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-i-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	//第一次插入
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithCapacity(1000), InsertWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}

	//刷新
	time.Sleep(1 * time.Second)
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	//不过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(1), c)

	//过期
	time.Sleep(2 * time.Second)
	c, err = cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_Cuckoofilter_Insert_ttl_firsttime(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-i-ttl-ft"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	//第一次插入
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithCapacity(1000), InsertWithTTLAtFirstTime(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}

	//刷新
	time.Sleep(1 * time.Second)
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithTTLAtFirstTime(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	//过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}
func Test_Cuckoofilter_Count_DEL(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-cf-cd"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewCuckoofilter error")
	}
	//一次设置
	_, err = bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//二次设置
	_, err = bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//3次设置
	_, err = bf.AddItem(ctx, "abc", AddWithoutNX())
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//计数
	num, err := bf.CountItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	assert.Equal(t, int64(3), num)
	//删除
	ok, err := bf.DelItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "DelItem error")
	}
	assert.Equal(t, true, ok)
	num, err = bf.CountItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	assert.Equal(t, int64(2), num)

	ok, err = bf.DelItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "DelItem error")
	}
	assert.Equal(t, true, ok)
	num, err = bf.CountItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	assert.Equal(t, int64(1), num)

	ok, err = bf.DelItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "DelItem error")
	}
	assert.Equal(t, true, ok)
	num, err = bf.CountItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "CountItem error")
	}
	assert.Equal(t, int64(0), num)

	ok, err = bf.DelItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "DelItem error")
	}
	assert.Equal(t, false, ok)
}
