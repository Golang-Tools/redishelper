package redishelper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

//ErrAlreadyLocked 该锁已经被锁定
var ErrAlreadyLocked = errors.New("该锁已经被锁定")

//ErrAlreadyUnLocked 该锁已经被解锁
var ErrAlreadyUnLocked = errors.New("该锁已经被解锁")

//ErrNoRightToUnLocked 无权解锁该锁
var ErrNoRightToUnLocked = errors.New("无权解锁该锁")

// Lock 分布式锁结构
type Lock struct {
	Key      string                //锁使用的key
	ClientID string                //客户端id
	MaxTTL   time.Duration         //锁的最大过期时间
	client   redis.UniversalClient //redis客户端对象
}

//New 新建一个锁对象
func New(client redis.UniversalClient, key, clientID string, maxttl time.Duration) *Lock {
	lock := new(Lock)
	lock.Key = key
	lock.ClientID = clientID
	lock.MaxTTL = maxttl
	lock.client = client
	return lock
}

//Lock 设置锁
func (l *Lock) Lock(ctx context.Context) error {
	set, err := l.client.SetNX(ctx, l.Key, l.ClientID, l.MaxTTL).Result()
	if err != nil {
		return err
	}
	if set == true {
		return nil
	}
	fmt.Println("set is ", set)
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
func (l *Lock) Wait(ctx context.Context, checkperiod time.Duration) error {
loop:
	for {
		r, err := l.Check(ctx)
		if err != nil {
			return err
		}
		if r {
			time.Sleep(checkperiod)
		} else {
			break loop
		}
	}
	return nil
}
