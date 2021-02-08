//Package bitmap bitmap操作支持
//bitmap可以用于分布式去重
//bitmap实现了一般set的常用接口(Add,Remove,Contained,Len,ToArray)
package bitmap

import (
	"context"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/go-redis/redis/v8"
)

//Bitmap 位图对象
type Bitmap struct {
	*clientkey.ClientKey
}

//New 创建一个新的位图对象
//@params k *key.Key redis客户端的键对象
func New(k **clientkey.ClientKey) *Bitmap {
	bm := new(Bitmap)
	bm.Key = k
	return bm
}

// 写操作

//Add 为bitmap中固定偏移量位置置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 要置1的偏移量
func (bm *Bitmap) Add(ctx context.Context, offset int64) error {
	if bm.Opt.MaxTTL != 0 {
		_, err := bm.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SetBit(ctx, bm.Key, offset, 1)
			pipe.Expire(ctx, bm.Key, bm.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := bm.client.SetBit(ctx, bm.Key, offset, 1).Result()
	if err != nil {
		return err
	}
	return nil
}

//SetM 为bitmap中多个偏移量位置置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offsets ...int64 要置1的偏移量
func (bm *Bitmap) SetM(ctx context.Context, offsets ...int64) error {
	_, err := bm.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, bm.Key, offset, 1)
		}
		if bm.MaxTTL != 0 {
			pipe.Expire(ctx, bm.Key, bm.MaxTTL)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//UnSet 为bitmap中固定偏移量位置置0
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 要置0的偏移量
func (bm *Bitmap) UnSet(ctx context.Context, offset int64) error {
	if bm.MaxTTL != 0 {
		_, err := bm.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SetBit(ctx, bm.Key, offset, 0)
			pipe.Expire(ctx, bm.Key, bm.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := bm.client.SetBit(ctx, bm.Key, offset, 0).Result()
	if err != nil {
		return err
	}
	return nil
}

//UnSetM 为bitmap中多个偏移量位置置0
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offsets ...int64 要置0的偏移量
func (bm *Bitmap) UnSetM(ctx context.Context, offsets ...int64) error {
	_, err := bm.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, bm.Key, offset, 0)
		}
		if bm.MaxTTL != 0 {
			pipe.Expire(ctx, bm.Key, bm.MaxTTL)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// 读操作

//IsSetted 检查bitmap某偏移量是否已经被置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 查看的偏移量
func (bm *Bitmap) IsSetted(ctx context.Context, offset int64) (bool, error) {
	if bm.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	res, err := bm.client.GetBit(ctx, bm.Key, offset).Result()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}

//CountSetted 检查bitmap中被置1的有多少位
//规定报错时返回的第一位是-1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params scop ...int64 最多2位,第一位表示开始位置,第二位表示结束位置
func (bm *Bitmap) CountSetted(ctx context.Context, scop ...int64) (int64, error) {
	if bm.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	lenScop := len(scop)
	switch lenScop {
	case 0:
		{
			res, err := bm.client.BitCount(ctx, bm.Key, nil).Result()
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
			res, err := bm.client.BitCount(ctx, bm.Key, &bc).Result()
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
			res, err := bm.client.BitCount(ctx, bm.Key, &bc).Result()
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
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (bm *Bitmap) SettedOffsets(ctx context.Context) ([]int64, error) {
	if bm.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	res := []int64{}
	bitmapstring, err := bm.client.Get(ctx, bm.Key).Result()
	if err != nil {
		if err == redis.Nil {
			return res, nil
		}
		return nil, err
	}
	bitmapbytes := []byte(bitmapstring)
	for key, value := range getBitSet(bitmapbytes) {
		if value == true {
			res = append(res, int64(key))
		}
	}
	return res, nil
}

// 别名 对应set的方法

//Add 对应set的add操作
func (bm *Bitmap) Add(ctx context.Context, value ...int64) error {
	return bm.SetM(ctx, value...)
}

//Remove 对应set的remove操作
func (bm *Bitmap) Remove(ctx context.Context, value ...int64) error {
	return bm.UnSetM(ctx, value...)
}

//Len 对应set的len操作
func (bm *Bitmap) Len(ctx context.Context) (int64, error) {
	return bm.CountSetted(ctx)
}

//Contained 对应set的Contained操作,也就是in运算符
func (bm *Bitmap) Contained(ctx context.Context, value int64) (bool, error) {
	return bm.IsSetted(ctx, value)
}

//ToArray set转换位array
func (bm *Bitmap) ToArray(ctx context.Context) ([]int64, error) {
	return bm.SettedOffsets(ctx)
}

type OpOption struct {
}

//Intersection 对应set的求交集操作
func (bm *Bitmap) Intersection(ctx context.Context, targetbmkey string, maxttl time.Duration, otherbms ...*Bitmap) (*Bitmap, error) {
	keys := []string{}
	for _, otherbm := range otherbms {
		keys = append(keys, otherbm.Key)
	}
	_, err := bm.client.BitOpOr(ctx, targetbmkey, bm.Key, keys...).Result()
	if err != nil {
		return nil, err
	}

	return
}

// union

// except
