//Package bitmap bitmap操作支持
//bitmap可以用于分布式去重
//bitmap实现了一般set的常用接口(Add,Remove,Contained,Len,ToArray)
package key

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/go-redis/redis/v8"
)

//Key 描述任意一种的单个key对象
type Key struct {
	Key    string
	MaxTTL time.Duration
	redis.UniversalClient
}

//New 创建一个新的key对象
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params maxttl ...time.Duration 最大存活时间,设置了就执行刷新
func NewKey(client redis.UniversalClient, key string, maxttl ...time.Duration) *Key {
	k := new(Key)
	k.UniversalClient = client
	k.Key = key
	switch len(maxttl) {
	case 0:
		{
			return k
		}
	case 1:
		{
			if maxttl[0] != 0 {
				k.MaxTTL = maxttl[0]
				return k
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return k
		}
	default:
		{
			log.Warn("ttl最多只能设置一个,使用第一个作为过期时间")
			if maxttl[0] != 0 {
				k.MaxTTL = maxttl[0]
				return k
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return k
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

//生命周期操作

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
	if k.MaxTTL != 0 {
		_, err := k.UniversalClient.Expire(ctx, k.Key, k.MaxTTL).Result()
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
