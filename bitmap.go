//Package redishelper 为代理添加bitmap操作支持
package redishelper

import (
	"context"

	"github.com/go-redis/redis/v8"
)

//BitmapIsSetted 检查bitmap某偏移量是否已经被置1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 查看的偏移量
func (proxy *redisHelper) BitmapIsSetted(ctx context.Context, key string, offset int64) (bool, error) {
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

//BitmapCountSetted 检查bitmap中被置1的有多少位
// 规定报错时返回的第一位是-1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params scop ...int64 最多2位,第一位表示开始位置,第二位表示结束位置
func (proxy *redisHelper) BitmapCountSetted(ctx context.Context, key string, scop ...int64) (int64, error) {
	if !proxy.IsOk() {
		return -1, ErrProxyNotYetSettedGoRedisV8Client
	}
	lenScop := len(scop)

	switch lenScop {
	case 0:
		{
			res, err := proxy.BitCount(ctx, key, nil).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	case 1:
		{
			bc := redis.BitCount{
				Start: scop[0],
			}
			res, err := proxy.BitCount(ctx, key, &bc).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	case 2:
		{
			bc := redis.BitCount{
				Start: scop[0],
				End:   scop[1],
			}
			res, err := proxy.BitCount(ctx, key, &bc).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	default:
		{
			return -1, ErrIndefiniteParameterLength
		}
	}
}

//BitmapSet 为bitmap中固定偏移量位置置1
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 要置1的偏移量
func (proxy *redisHelper) BitmapSet(ctx context.Context, key string, offset int64) error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedGoRedisV8Client
	}
	_, err := proxy.SetBit(ctx, key, offset, 1).Result()
	if err != nil {
		return err
	}
	return nil
}

//BitmapSetM 为bitmap中多个偏移量位置置1
// 该操作使用TxPipeline,是原子操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 要置1的偏移量
func (proxy *redisHelper) BitmapSetM(ctx context.Context, key string, offsets ...int64) error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedGoRedisV8Client
	}
	_, err := proxy.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, key, offset, 1)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//BitmapUnSet 为bitmap中固定偏移量位置置0
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 要置1的偏移量
func (proxy *redisHelper) BitmapUnSet(ctx context.Context, key string, offset int64) error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedGoRedisV8Client
	}
	_, err := proxy.SetBit(ctx, key, offset, 0).Result()
	if err != nil {
		return err
	}
	return nil
}

//BitmapUnSetM 为bitmap中多个偏移量位置置0
// 该操作使用TxPipeline,是原子操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params offset int64 要置1的偏移量
func (proxy *redisHelper) BitmapUnSetM(ctx context.Context, key string, offsets ...int64) error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedGoRedisV8Client
	}
	_, err := proxy.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, key, offset, 0)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func hasBit(n byte, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

func getBitSet(redisResponse []byte) []bool {
	bitset := make([]bool, len(redisResponse)*8)
	for i := range redisResponse {
		for j := 7; j >= 0; j-- {
			bitn := uint(i*8 + (7 - j))
			bitset[bitn] = hasBit(redisResponse[i], uint(j))
		}
	}
	return bitset
}

//SettedOffsets 检查哪些偏移量是已经被置1的
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
func (proxy *redisHelper) BitmapSettedOffsets(ctx context.Context, key string) ([]int64, error) {
	if !proxy.IsOk() {
		return nil, ErrProxyNotYetSettedGoRedisV8Client
	}
	bitmapstring, err := proxy.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	bitmapbytes := []byte(bitmapstring)
	res := []int64{}
	for key, value := range getBitSet(bitmapbytes) {
		if value == true {
			res = append(res, int64(key))
		}
	}
	return res, nil
}
