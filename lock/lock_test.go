package lock

import (
	"context"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_new_Lock_err(t *testing.T) {
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
	_, err = New(cli, "testlock", "client01", 10*time.Second, 10*time.Second)
	if err != nil {
		assert.Equal(t, err, ErrArgCheckPeriodMoreThan1)
	}
	_, err = New(cli, "testlock", "client02", 10*time.Microsecond)
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
}

func Test_lock_Lock(t *testing.T) {
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
	lock, err := New(cli, "testlock", "client01", 10*time.Second)
	if err != nil {
		assert.Error(t, err, "new lock error")
	}
	lock2, err := New(cli, "testlock", "client02", 10*time.Second)
	if err != nil {
		assert.Error(t, err, "new lock2 error")
	}
	err = lock.Lock(ctx)
	if err != nil {
		assert.Error(t, err, "lock error")
	}
	err = lock.Lock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyLocked)
	}
	err = lock2.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrNoRightToUnLocked)
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.Error(t, err, "该锁已经被解锁")
	}
	err = lock.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyUnLocked)
	}
}

func Test_lock_waitLock(t *testing.T) {
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
	lock, err := New(cli, "testlock", "client01", 10*time.Second)
	if err != nil {
		assert.Error(t, err, "new lock error")
	}
	lock2, err := New(cli, "testlock", "client02", 10*time.Second)
	if err != nil {
		assert.Error(t, err, "new lock2 error")
	}

	locked, err := lock2.Check(ctx)
	if err != nil {
		assert.Error(t, err, "is locked error")
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
		assert.Error(t, err, "lock2 wait error")
	}
	t2 := time.Now().Unix()
	assert.LessOrEqual(t, int64(2), t2-t1)
}
