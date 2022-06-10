package lock

import (
	"context"
	"strings"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

//TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackgroundClient(t *testing.T) (redis.UniversalClient, context.Context) {
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
	return cli, ctx
}

func Test_new_Lock_err(t *testing.T) {
	// 准备工作
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	_, err := New(ck, WithCheckPeriod(10*time.Microsecond))
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}

}

func Test_new_Lock_defaultkey(t *testing.T) {
	// 准备工作
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	l, err := New(ck)
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
	assert.Equal(t, true, strings.HasPrefix(l.Key(), "redishelper::lock::"))
}

func Test_new_Lock_fix_prefix(t *testing.T) {
	// 准备工作
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	l, err := New(ck, WithNamespace("test-prefix"))
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
	assert.Equal(t, true, strings.HasPrefix(l.Key(), "test-prefix::"))
}

func Test_new_Lock_fix_key(t *testing.T) {
	// 准备工作
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	l, err := New(ck, WithSpecifiedKey("test-key"))
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
	assert.Equal(t, false, strings.HasPrefix(l.Key(), "redis-helper-lock::"))
}
func Test_new_Lock_fix_key_and_prefix(t *testing.T) {
	// 准备工作
	ck, _ := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	l, err := New(ck, WithSpecifiedKey("test-key"), WithNamespace("test-prefix"))
	if err != nil {
		assert.Equal(t, err, ErrCheckPeriodLessThan100Microsecond)
	}
	assert.Equal(t, "test-key", l.Key())
}

func Test_lock_Lock(t *testing.T) {
	// 准备工作
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	key := "test_lock"
	lock, err := New(ck, WithClientID("client01"), WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock error")
	}
	lock2, err := New(ck, WithClientID("client02"), WithSpecifiedKey(key))
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
		assert.FailNow(t, "lock lock not get error")
	}
	err = lock2.Lock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrAlreadyLocked)
	} else {
		assert.FailNow(t, "lock2 lock not get error")
	}
	err = lock2.Unlock(ctx)
	if err != nil {
		assert.Equal(t, err, ErrNoRightToUnLock)
	} else {
		assert.FailNow(t, "lock2 unlock not get error")
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
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	key := "test_lock"

	lock, err := New(ck, WithClientID("client01"), WithSpecifiedKey(key))
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock error")
	}
	lock2, err := New(ck, WithClientID("client02"), WithSpecifiedKey(key))
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

func Test_lock_waitLock_maxttl(t *testing.T) {
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	//开始测试
	key := "test_lock"

	lock, err := New(ck, WithClientID("client01"), WithSpecifiedKey(key), WithMaxTTL(2*time.Second))
	if err != nil {
		assert.FailNow(t, err.Error(), "new lock error")
	}
	locked, err := lock.Check(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "is locked error")
	}
	assert.Equal(t, false, locked)
	err = lock.Lock(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "lock lock error")
	}
	t1 := time.Now().Unix()
	locked, err = lock.Check(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "is locked error")
	}
	assert.Equal(t, true, locked)
	err = lock.Wait(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "lock wait error")
	}
	t2 := time.Now().Unix()
	assert.LessOrEqual(t, int64(2), t2-t1)
	locked, err = lock.Check(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "is locked error")
	}
	assert.Equal(t, false, locked)
}
