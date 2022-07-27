package bloomfilterhelper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_bloompipe_ADD(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe"
	pipe := cli.Pipeline()
	add1, err := AddItemPipe(pipe, ctx, key, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItemPipe error")
	}
	add2, err := AddItemPipe(pipe, ctx, key, "a")
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItemPipe error")
	}
	exit1 := ExistsItemPipe(pipe, ctx, key, "a")
	exit2 := ExistsItemPipe(pipe, ctx, key, "b")
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	addr1, err := add1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add1.Result error")
	}
	assert.Equal(t, false, addr1)
	addr2, err := add2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add2.Result error")
	}
	assert.Equal(t, true, addr2)

	exitr1, err := exit1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exit1.Result error")
	}
	assert.Equal(t, true, exitr1)
	exitr2, err := exit2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exit1.Result error")
	}
	assert.Equal(t, false, exitr2)
}
func Test_bloompipe_ADD_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-ttl"
	pipe := cli.Pipeline()
	add1, err := AddItemPipe(pipe, ctx, key, "a", AddWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "AddItemPipe error")
	}
	exit1 := ExistsItemPipe(pipe, ctx, key, "a")
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	addr1, err := add1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add1.Result error")
	}
	assert.Equal(t, false, addr1)
	exitr1, err := exit1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exit1.Result error")
	}
	assert.Equal(t, true, exitr1)

	// 过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_bloompipe_MADD(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-madd"
	pipe := cli.Pipeline()
	add1, err := MAddItemPipe(pipe, ctx, key, []string{"a", "b", "c"})
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItemPipe error")
	}
	add2, err := MAddItemPipe(pipe, ctx, key, []string{"a", "b", "c"})
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItemPipe error")
	}
	exit1, err := MExistsItemPipe(pipe, ctx, key, "a", "b", "c")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItemPipe error")
	}
	exit2, err := MExistsItemPipe(pipe, ctx, key, "d", "e", "f")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItemPipe error")
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}

	addr1, err := add1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add1.Result error")
	}
	assert.Equal(t, false, addr1["a"])
	assert.Equal(t, false, addr1["b"])
	assert.Equal(t, false, addr1["c"])

	addr2, err := add2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add2.Result error")
	}
	assert.Equal(t, true, addr2["a"])
	assert.Equal(t, true, addr2["b"])
	assert.Equal(t, true, addr2["c"])

	exitr1, err := exit1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exit1.Result error")
	}
	assert.Equal(t, true, exitr1["a"])
	assert.Equal(t, true, exitr1["b"])
	assert.Equal(t, true, exitr1["c"])
	exitr2, err := exit2.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exit1.Result error")
	}
	assert.Equal(t, false, exitr2["d"])
	assert.Equal(t, false, exitr2["e"])
	assert.Equal(t, false, exitr2["f"])
}

func Test_bloompipe_MADD_ttl(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-madd"
	pipe := cli.Pipeline()
	add1, err := MAddItemPipe(pipe, ctx, key, []string{"a", "b", "c"}, AddWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "MAddItemPipe error")
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}

	addr1, err := add1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "add1.Result error")
	}
	assert.Equal(t, false, addr1["a"])
	assert.Equal(t, false, addr1["b"])
	assert.Equal(t, false, addr1["c"])
	// 过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_bloompipe_Reserve_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-ri"
	pipe := cli.Pipeline()
	ReservePipe(pipe, ctx, key, 1000, 0.001, ReserveWithExpansion(1))
	inser1, err := InsertPipe(pipe, ctx, key, []string{"abc", "cde", "def"})
	if err != nil {
		assert.FailNow(t, err.Error(), "InsertPipe error")
	}
	exist1, err := MExistsItemPipe(pipe, ctx, key, "abc", "cde", "def")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItemPipe error")
	}
	resi := InfoPipe(pipe, ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	ir1, err := inser1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "inser1.Result error")
	}
	assert.Equal(t, false, ir1["abc"])
	assert.Equal(t, false, ir1["cde"])
	assert.Equal(t, false, ir1["def"])

	er1, err := exist1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exist1.Result error")
	}
	assert.Equal(t, true, er1["abc"])
	assert.Equal(t, true, er1["cde"])
	assert.Equal(t, true, er1["def"])
	resinfo, err := resi.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "resinfo.Result error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(1), resinfo.ExpansionRate)
	assert.Equal(t, int64(1000), resinfo.Capacity)

}

func Test_bloompipe_Reserve_Insert_ttl_noscal(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-ri-noscal"
	pipe := cli.Pipeline()
	ReservePipe(pipe, ctx, key, 1000, 0.001, ReserveWithNonScaling(), ReserveWithTTL(2*time.Second))
	inser1, err := InsertPipe(pipe, ctx, key, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "InsertPipe error")
	}
	exist1, err := MExistsItemPipe(pipe, ctx, key, "abc", "cde", "def")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItemPipe error")
	}
	resi := InfoPipe(pipe, ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}
	ir1, err := inser1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "inser1.Result error")
	}
	assert.Equal(t, false, ir1["abc"])
	assert.Equal(t, false, ir1["cde"])
	assert.Equal(t, false, ir1["def"])

	er1, err := exist1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exist1.Result error")
	}
	assert.Equal(t, true, er1["abc"])
	assert.Equal(t, true, er1["cde"])
	assert.Equal(t, true, er1["def"])
	resinfo, err := resi.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "resinfo.Result error")
	}
	assert.Equal(t, int64(3), resinfo.NumberOfItemsInserted)
	assert.Equal(t, int64(0), resinfo.ExpansionRate)
	assert.Equal(t, int64(1000), resinfo.Capacity)

	//过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}

func Test_bloompipe_Insert_nocreateerr(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-ri-nocreateerr"
	pipe := cli.Pipeline()
	_, err := InsertPipe(pipe, ctx, key, []string{"abc", "cde", "def"}, InsertWithNoCreate())
	if err != nil {
		assert.FailNow(t, err.Error(), "InsertPipe error")
	}
	_, err = pipe.Exec(ctx)
	assert.EqualError(t, err, "ERR not found")
}
func Test_bloompipe_Insert(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	key := "test-bloom-pipe-ri-nocreateerr"
	pipe := cli.Pipeline()

	inser1, err := InsertPipe(pipe, ctx, key, []string{"abc", "cde", "def"}, InsertWithCapacity(1000), InsertWithErrorRate(0.001), InsertWithExpansion(1), InsertWithTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "InsertPipe error")
	}
	exist1, err := MExistsItemPipe(pipe, ctx, key, "abc", "cde", "def")
	if err != nil {
		assert.FailNow(t, err.Error(), "MExistsItem error")
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "pipe.Exec error")
	}

	ir1, err := inser1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "inser1.Result error")
	}
	assert.Equal(t, false, ir1["abc"])
	assert.Equal(t, false, ir1["cde"])
	assert.Equal(t, false, ir1["def"])

	er1, err := exist1.Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "exist1.Result error")
	}
	assert.Equal(t, true, er1["abc"])
	assert.Equal(t, true, er1["cde"])
	assert.Equal(t, true, er1["def"])
	//过期
	time.Sleep(3 * time.Second)
	c, err := cli.Exists(ctx, key).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "key Exists error")
	}
	assert.Equal(t, int64(0), c)
}
