//Package lock 分布式锁实现
//分布式锁需要有客户端信息,只可以获得锁的客户端自己解锁或者等待锁自己过期
//当未能获得锁需要等待锁释放时可以通过wait接口实现
package lock

import (
	"context"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
)

//DefaultCheckPeriod 等待的轮询间隔默认500微秒
const DefaultCheckPeriod = 500 * time.Microsecond

//MiniCheckPeriod 等待的轮询间隔最低100微秒
const MiniCheckPeriod = 100 * time.Microsecond

// Lock 分布式锁结构
type Lock struct {
	ClientID    string        //客户端id
	CheckPeriod time.Duration //等待的轮询间隔
	*clientkey.ClientKey
}

//New 新建一个锁对象
func New(k *clientkey.ClientKey, clientID string, checkperiod ...time.Duration) (*Lock, error) {
	lock := new(Lock)
	lock.ClientKey = k
	lock.ClientID = clientID
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

//写操作

//Lock 设置锁
func (l *Lock) Lock(ctx context.Context) error {
	set, err := l.Client.SetNX(ctx, l.Key, l.ClientID, l.Opt.MaxTTL).Result()
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
	// clientid, err := l.Client.Get(ctx, l.Key).Result()
	// if err != nil {
	// 	if err == redis.Nil {
	// 		// key不存在,不是锁定状态
	// 		return ErrAlreadyUnLocked
	// 	}
	// 	return err
	// }
	// if clientid != l.ClientID {
	// 	return ErrNoRightToUnLocked
	// }
	// _, err = l.Client.Del(ctx, l.Key).Result()
	// if err != nil {
	// 	return err
	// }
	// return nil

	// 1表示删成功
	// 2表示key不存在
	// 3表示key不匹配
	res, err := l.Client.Eval(ctx, `
		if redis.call("GET", KEYS[1])
		then 
			if redis.call("GET", KEYS[1]) == ARGV[1]
			then
				return redis.call("DEL", KEYS[1])
			else
				return 3
		end
		else
			return 2
		end
	`, []string{l.Key}, l.ClientID).Result()
	if err != nil {
		return err
	}
	switch res.(int64) {
	case 2:
		{
			return ErrAlreadyUnLocked
		}
	case 3:
		{
			return ErrNoRightToUnLocked
		}
	default:
		{
			return nil
		}
	}
}

//读操作

//Check 检测是否是锁定状态,true为锁定状态,false为非锁定状态
func (l *Lock) Check(ctx context.Context) (bool, error) {
	r, err := l.Client.Exists(ctx, l.Key).Result()
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
