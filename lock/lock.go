//Package lock 分布式锁实现
//分布式锁需要有客户端信息,只可以获得锁的客户端自己解锁或者等待锁自己过期
//当未能获得锁需要等待锁释放时可以通过wait接口实现
package lock

import (
	"context"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//MiniCheckPeriod 等待的轮询间隔最低100微秒
const MiniCheckPeriod = 100 * time.Microsecond

// Lock 分布式锁结构
type Lock struct {
	opt Options
	*middlewarehelper.MiddleWareAbc
	*clientIdhelper.ClientIDAbc
}

//New 新建一个锁对象
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Lock, error) {
	l := new(Lock)
	l.opt = defaultOptions
	optparams.GetOption(&l.opt, opts...)
	if l.opt.CheckPeriod < MiniCheckPeriod {
		return nil, ErrCheckPeriodLessThan100Microsecond
	}
	meta, err := middlewarehelper.New(cli, "lock", l.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	cid, err := clientIdhelper.New(l.opt.ClientIDOpts...)
	l.MiddleWareAbc = meta
	l.ClientIDAbc = cid
	return l, nil
}

//写操作

//Lock 设置锁
func (l *Lock) Lock(ctx context.Context) error {
	set, err := l.Client().SetNX(ctx, l.Key(), l.ClientID(), l.MaxTTL()).Result()
	if err != nil {
		return err
	}
	if set {
		return nil
	}
	return ErrAlreadyLocked
}

//Unlock 释放锁,已经释放锁或无权释放锁时报错
func (l *Lock) Unlock(ctx context.Context) error {
	// 1表示删成功
	// 2表示key不存在
	// 3表示key不匹配
	lunlockscript := redis.NewScript(`
		if redis.call("EXISTS", KEYS[1]) == 1 then 
			if redis.call("GET", KEYS[1]) == ARGV[1] then
				return redis.call("DEL", KEYS[1])
			else
				return 3
			end
		else
			return 2
		end`)
	res, err := lunlockscript.Run(ctx, l.Client(), []string{l.Key()}, l.ClientID()).Result()
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
			return ErrNoRightToUnLock
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
	r, err := l.Client().Exists(ctx, l.Key()).Result()
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
			time.Sleep(l.opt.CheckPeriod)
		} else {
			break loop
		}
	}
	return nil
}
