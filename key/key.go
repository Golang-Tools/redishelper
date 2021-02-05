//Package bitmap bitmap操作支持
//bitmap可以用于分布式去重
//bitmap实现了一般set的常用接口(Add,Remove,Contained,Len,ToArray)
package key

import (
	"context"
	"errors"
	"time"

	"github.com/Golang-Tools/redishelper/utils"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//Key 描述任意一种的单个key对象
type Key struct {
	Key             string
	Opt             *Option
	autorefreshtask *cron.Cron //定时任务对象
	redis.UniversalClient
}

//Option 设置key行为的选项
//@attribute MaxTTL time.Duration 为0则不设置过期
//@attribute AutoRefresh string 需要为crontab格式的字符串,否则不会自动定时刷新
type Option struct {
	MaxTTL              time.Duration
	AutoRefreshInterval string
}

//New 创建一个新的key对象
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params opts ...*KeyOption key的选项
func New(client redis.UniversalClient, key string, opts ...*Option) (*Key, error) {
	k := new(Key)
	k.UniversalClient = client
	k.Key = key
	switch len(opts) {
	case 0:
		{
			k.Opt = &Option{}
			return k, nil
		}
	case 1:
		{
			opt := opts[0]
			k.Opt = opt
			return k, nil
		}
	default:
		{
			return nil, utils.ErrParamOptsLengthMustLessThan2
		}
	}
}

//Exists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Exists(ctx context.Context) (bool, error) {
	_, err := k.UniversalClient.Exists(ctx, k.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

//Type 查看key的类型
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Type(ctx context.Context) (string, error) {
	ok, err := k.Exists(ctx)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrKeyNotExist
	}
	typeName, err := k.UniversalClient.Type(ctx, k.Key).Result()
	return typeName, err
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) TTL(ctx context.Context) (time.Duration, error) {
	ok, err := k.Exists(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, ErrKeyNotExist
	}
	res, err := k.UniversalClient.TTL(ctx, k.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//写操作

//Delete 删除key
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Delete(ctx context.Context) (bool, error) {
	ok, err := k.Exists(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrKeyNotExist
	}
	_, err = k.UniversalClient.Del(ctx, k.Key).Result()
	if err != nil {
		return false, err
	}
	return true, nil
}

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) RefreshTTL(ctx context.Context) error {
	ok, err := k.Exists(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrKeyNotExist
	}
	if k.Opt.MaxTTL != 0 {
		_, err := k.UniversalClient.Expire(ctx, k.Key, k.Opt.MaxTTL).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrKeyNotSetMaxTLL
}

//AutoRefresh 自动刷新key的过期时间
func (k *Key) AutoRefresh() error {
	if k.autorefreshtask != nil {
		return ErrAutoRefreshTaskHasBeenSet
	}
	k.autorefreshtask = cron.New()
	_, err := k.autorefreshtask.AddFunc(k.Opt.AutoRefreshInterval, func() {
		ctx := context.Background()
		_, err := c.Update(ctx)
		if err != nil {

		}
	})
	c.c.Start()
	return nil
}

//StopAutoRefreshTTL 取消自动更新缓存
func (k *Key) StopAutoRefresh() error {
	if c.UpdatePeriod == "" || c.c == nil {
		return errors.New("自动更新未启动")
	}
	c.c.Stop()
	c.c = nil
	return nil
}
