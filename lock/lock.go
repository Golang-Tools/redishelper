//Package lock 分布式锁实现
//分布式锁需要有客户端信息,只可以获得锁的客户端自己解锁或者等待锁自己过期
//当未能获得锁需要等待锁释放时可以通过wait接口实现
package lock

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

//DefaultCheckPeriod 等待的轮询间隔默认500微秒
const DefaultCheckPeriod = 500 * time.Microsecond

//MiniCheckPeriod 等待的轮询间隔最低100微秒
const MiniCheckPeriod = 100 * time.Microsecond

//Canlock 锁对象的接口
type Canlock interface {
	Lock(context.Context) error
	Unlock(context.Context) error
	Wait(context.Context) error
}

// Lock 分布式锁结构
type Lock struct {
	Key         string                //锁使用的key
	ClientID    string                //客户端id
	MaxTTL      time.Duration         //锁的最大过期时间
	CheckPeriod time.Duration         //等待的轮询间隔
	client      redis.UniversalClient //redis客户端对象
}

//New 新建一个锁对象
func New(client redis.UniversalClient, key, clientID string, maxttl time.Duration, checkperiod ...time.Duration) (*Lock, error) {
	lock := new(Lock)
	lock.Key = key
	lock.ClientID = clientID
	lock.MaxTTL = maxttl
	lock.client = client
	switch len(checkperiod) {
	case 0:
		{
			lock.CheckPeriod = DefaultCheckPeriod
		}
	case 1:
		{
			cp := checkperiod[0]
			if cp < MiniCheckPeriod {
				return nil, ErrCheckPeriodLessThan100Microsecond
			}
			lock.CheckPeriod = cp
		}
	default:
		{
			return nil, ErrArgCheckPeriodMoreThan1
		}
	}
	return lock, nil
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *Lock) RefreshTTL(ctx context.Context) error {
	if l.MaxTTL != 0 {
		_, err := l.client.Expire(ctx, l.Key, l.MaxTTL).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrLockNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	_, err := l.client.Exists(ctx, l.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
		return 0, ErrKeyNotExist
	}
	res, err := l.client.TTL(ctx, l.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//写操作

//Lock 设置锁
func (l *Lock) Lock(ctx context.Context) error {
	set, err := l.client.SetNX(ctx, l.Key, l.ClientID, l.MaxTTL).Result()
	if err != nil {
		return err
	}
	if set == true {
		return nil
	}
	return ErrAlreadyLocked
}

//Unlock 释放锁,已经释放锁或无权释放锁时报错
func (l *Lock) Unlock(ctx context.Context) error {
	clientid, err := l.client.Get(ctx, l.Key).Result()
	if err != nil {
		if err == redis.Nil {
			// key不存在,不是锁定状态
			return ErrAlreadyUnLocked
		}
		return err
	}
	if clientid != l.ClientID {
		return ErrNoRightToUnLocked
	}
	_, err = l.client.Del(ctx, l.Key).Result()
	if err != nil {
		return err
	}
	return nil
}

//读操作

//Check 检测是否是锁定状态,true为锁定状态,false为非锁定状态
func (l *Lock) Check(ctx context.Context) (bool, error) {
	r, err := l.client.Exists(ctx, l.Key).Result()
	if err != nil {
		return false, err
	}
	if r == 0 {
		return false, nil
	}
	return true, nil
}

//Wait 等待锁释放
func (l *Lock) Wait(ctx context.Context) error {
loop:
	for {
		r, err := l.Check(ctx)
		if err != nil {
			return err
		}
		if r {
			time.Sleep(l.CheckPeriod)
		} else {
			break loop
		}
	}
	return nil
}
