package redishelper

import (
	"context"
)

type Cachefunc func() (string, []byte)

//BitmapIsSetted 检查bitmap某偏移量是否已经被置1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 查看的偏移量
func (proxy *redisHelper) Cache(ctx context.Context, fn Cachefunc) (string, error) {
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
