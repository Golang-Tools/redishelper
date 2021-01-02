package redishelper

import (
	"context"

	"github.com/go-redis/redis"
)

//Cachefunc 缓存函数结果
type Cachefunc func() []byte

//CacheFuncttion 检查bitmap某偏移量是否已经被置1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 查看的偏移量
func (proxy *redisHelper) CacheFuncttion(ctx context.Context, key string, refresh bool, fn Cachefunc) ([]byte, error) {
	if !proxy.IsOk() {
		return nil, ErrProxyNotYetSettedGoRedisV8Client
	}
	res, err := proxy.Get(ctx, key).Result()
	switch err {
	case nil:
		{
			return []byte(res), nil
		}
	case redis.Nil:
		{

		}
	default:
		{
			return nil, err
		}
	}

}

//BitmapIsSetted 检查bitmap某偏移量是否已经被置1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 查看的偏移量
func (proxy *redisHelper) CacheTTL(ctx context.Context, key string, offset int64) (bool, error) {
	if !proxy.IsOk() {
		return false, ErrProxyNotYetSettedGoRedisV8Client
	}
	res, err := proxy.GetBit(ctx, key, offset).Result()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}
