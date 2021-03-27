package lock

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/clientkey"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, keyname string, opts ...clientkey.Option) (*clientkey.ClientKey, context.Context) {
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
	key := clientkey.New(cli, keyname, opts...)
	fmt.Println("prepare task done")
	return key, ctx
}

func Test_new_Lock_err(t *testing.T) {
	// 准备工作
	key, _ := NewBackground(t, "test_lock")
	defer key.Client.Close()
	//开始测试
	_, err := New(key, "client01", 10*time.Second, 10*time.Second)
	if err != nil {
		assert.Equal(t, err, ErrArgCheckPeriodMoreThan1)
	}
	_, err = New(key, "client02", 10*time.Microsecond)
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
}

func Test_lock_Lock(t *testing.T) {
	// 准备工作
	key, ctx := NewBackground(t, "test_lock")
	defer key.Client.Close()
	//开始测试

	lock, err := New(key, "client01", 10*time.Second)
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock error")
	}
	lock2, err := New(key, "client02", 10*time.Second)
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock2 error")
	}

	err = lock.Lock(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "lock error")
	}
	err = lock.Lock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyLocked)
	} else {
		assert.FailNow(t, "not get error")
	}
	err = lock2.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrNoRightToUnLocked)
	} else {
		assert.FailNow(t, "not get error")
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "该锁已经被解锁")
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyUnLocked)
	} else {
		assert.FailNow(t, "not get error")
	}
}

func Test_lock_waitLock(t *testing.T) {
	// 准备工作
	key, ctx := NewBackground(t, "test_lock")
	defer key.Client.Close()
	//开始测试

	lock, err := New(key, "client01", 10*time.Second)
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock error")
	}
	lock2, err := New(key, "client02", 10*time.Second)
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock2 error")
	}

	locked, err := lock2.Check(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "is locked error")
	}
	assert.Equal(t, false, locked)
	go func() error {
		log.Info("to wait start")
		err := lock.Lock(ctx)
		if err != nil {
			log.Error("to wait error", log.Dict{"err": err.Error()})
			return err
		}
		defer lock.Unlock(ctx)
		time.Sleep(3 * time.Second)
		log.Info("to wait end")
		return nil
	}()
	time.Sleep(1 * time.Second)
	t1 := time.Now().Unix()
	err = lock2.Wait(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "lock2 wait error")
	}
	t2 := time.Now().Unix()
	assert.LessOrEqual(t, int64(2), t2-t1)
}
