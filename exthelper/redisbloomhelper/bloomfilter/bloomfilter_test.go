package bloomfilterhelper

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

//Test_KeyspaceNotification_Sync 测试同步配置
func Test_bloom_ADD(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	res, err := bf.AddItem(ctx, "abc")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
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
	res, err = bf.ExistsItem(ctx, "abcd")
	if err != nil {
		assert.FailNow(t, err.Error(), "ExistsItem error")
	}
	assert.Equal(t, false, res)
}

func Test_bloom_ADD_With_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	res, err := bf.AddItem(ctx, "abc", AddWithTTL(2*time.Second))
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
	res, err = bf.AddItem(ctx, "abcd", AddWithTTL(2*time.Second))
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

func Test_bloom_ADD_With_ttl_firsttime(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-first"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	res, err := bf.AddItem(ctx, "abc", AddWithTTLAtFirstTime(2*time.Second))
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
	res, err = bf.AddItem(ctx, "abcd", AddWithTTLAtFirstTime(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	assert.Equal(t, false, res)
	//过期
	time.Sleep(2 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_bloom_ADDM(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-m"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
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
}

func Test_bloom_ADDM_withttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-m-ttl"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	//刷新
	time.Sleep(1 * time.Second)
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTL(2*time.Second))
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

func Test_bloom_ADDM_withttl_firsttime(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-m-ttl-first"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTLAtFirstTime(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	//刷新(不会刷新)
	time.Sleep(1 * time.Second)
	_, err = bf.MAddItem(ctx, []string{"abc", "cde", "def"}, AddWithTTLAtFirstTime(2*time.Second))
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

func Test_bloom_Reserve_Insert_ttl_noscal(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-ri-ttl-noscal"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	err = bf.Reserve(ctx, 1000, 0.001, ReserveWithNonScaling(), ReserveWithTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
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
	assert.Equal(t, int64(0), resinfo.ExpansionRate)
	assert.Equal(t, int64(1000), resinfo.Capacity)
}
func Test_bloom_Reserve_Insert_expansion(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-ri-expansion"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	err = bf.Reserve(ctx, 1000, 0.001, ReserveWithExpansion(1))
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
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
	assert.Equal(t, int64(1), resinfo.ExpansionRate)
	assert.Equal(t, int64(1000), resinfo.Capacity)
}

func Test_bloom_Reserve_mtl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-r-mtl"
	bf, err := New(cli, WithSpecifiedKey(key), WithMaxTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	err = bf.Reserve(ctx, 1000, 0.001, ReserveWithExpansion(1), ReserveWithRefreshTTL())
	if err != nil {
		assert.FailNow(t, err.Error(), "Reserve error")
	}
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItem error")
	}
	assert.Equal(t, false, res["abc"])
	assert.Equal(t, false, res["cde"])
	assert.Equal(t, false, res["def"])
	//刷新(不会刷新)
	time.Sleep(1 * time.Second)
	_, err = bf.AddItem(ctx, "abcd", AddWithTTLAtFirstTime(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItem error")
	}
	//过期
	time.Sleep(1 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_bloom_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-i"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}
	// nocreate插入,失败
	_, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	assert.EqualError(t, err, "ERR not found")
	//第二次插入,成功
	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithCapacity(1000), InsertWithErrorRate(0.001), InsertWithExpansion(1), InsertWithTTLAtFirstTime(3*time.Second))
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
	//第三次插入
	res, err = bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithNoCreate(), InsertWithTTL(3*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, true, res["def"])

	//第4次插入
	res, err = bf.Insert(ctx, []string{"abc", "cde", "df"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "Insert error")
	}
	assert.Equal(t, true, res["abc"])
	assert.Equal(t, true, res["cde"])
	assert.Equal(t, false, res["df"])

	resinfo, err := bf.Info(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "Info error")
	}
	assert.Equal(t, int64(4), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(1000), resinfo.Capacity)
	assert.Equal(t, int64(1), resinfo.ExpansionRate)
	//删除key
	bf.Clean(ctx)
	exists, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), exists)
}

func Test_bloom_Insert_noscale(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bf-i-noscale"
	bf, err := New(cli, WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewBloomFilter error")
	}

	res, err := bf.Insert(ctx, []string{"abc", "cde", "def"}, InsertWithCapacity(1000), InsertWithErrorRate(0.001), InsertWithNonScaling(), InsertWithTTLAtFirstTime(3*time.Second))
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
	assert.Equal(t, int64(1000), resinfo.Capacity)
	assert.Equal(t, int64(0), resinfo.ExpansionRate)
	//删除key
	bf.Clean(ctx)
	exists, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), exists)
}
