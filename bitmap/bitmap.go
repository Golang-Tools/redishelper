//Package bitmap bitmap操作支持
//bitmap可以用于分布式去重
//bitmap实现了一般set的常用接口(Add,Remove,Contained,Len,ToArray)
package bitmap

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/exception"
	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
)

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

//Bitmap 位图对象
type Bitmap struct {
	*clientkey.ClientKey
}

//New 创建一个新的位图对象
//@params k *key.Key redis客户端的键对象
func New(k *clientkey.ClientKey) *Bitmap {
	bm := new(Bitmap)
	bm.ClientKey = k
	return bm
}

// 写操作

//Add 为bitmap中固定偏移量位置置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 要置1的偏移量
func (bm *Bitmap) Add(ctx context.Context, offset int64) error {
	if bm.Opt.MaxTTL != 0 {
		_, err := bm.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SetBit(ctx, bm.Key, offset, 1)
			pipe.Expire(ctx, bm.Key, bm.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := bm.Client.SetBit(ctx, bm.Key, offset, 1).Result()
	if err != nil {
		return err
	}
	return nil
}

//AddM 为bitmap中多个偏移量位置置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offsets ...int64 要置1的偏移量
func (bm *Bitmap) AddM(ctx context.Context, offsets ...int64) error {
	_, err := bm.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, bm.Key, offset, 1)
		}
		if bm.Opt.MaxTTL != 0 {
			pipe.Expire(ctx, bm.Key, bm.Opt.MaxTTL)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//Remove 为bitmap中固定偏移量位置置0
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 要置0的偏移量
func (bm *Bitmap) Remove(ctx context.Context, offset int64) error {
	if bm.Opt.MaxTTL != 0 {
		_, err := bm.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SetBit(ctx, bm.Key, offset, 0)
			pipe.Expire(ctx, bm.Key, bm.Opt.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := bm.Client.SetBit(ctx, bm.Key, offset, 0).Result()
	if err != nil {
		return err
	}
	return nil
}

//RemoveM 为bitmap中多个偏移量位置置0
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offsets ...int64 要置0的偏移量
func (bm *Bitmap) RemoveM(ctx context.Context, offsets ...int64) error {
	_, err := bm.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, offset := range offsets {
			pipe.SetBit(ctx, bm.Key, offset, 0)
		}
		if bm.Opt.MaxTTL != 0 {
			pipe.Expire(ctx, bm.Key, bm.Opt.MaxTTL)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// 读操作

//Contained 检查bitmap某偏移量是否已经被置1
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params offset int64 查看的偏移量
func (bm *Bitmap) Contained(ctx context.Context, offset int64) (bool, error) {
	if bm.Opt.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	res, err := bm.Client.GetBit(ctx, bm.Key, offset).Result()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}

//ScopCount 检查bitmap中特定范围内被置1的有多少位
//规定报错时返回的第一位是-1
//如果key设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params scop ...int64 最多2位,第一位表示开始位置,第二位表示结束位置,注意参数scop只能精确到bit,
//也就是说,0到7都表示实际填入的0;8,15都表示实际填入的1...,如果只有一位scop则表示查看某一位内的个数
func (bm *Bitmap) ScopCount(ctx context.Context, scop ...int64) (int64, error) {
	if bm.Opt.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	lenScop := len(scop)
	switch lenScop {
	case 0:
		{
			res, err := bm.Client.BitCount(ctx, bm.Key, nil).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	case 1:
		{
			startbit := scop[0]
			endbit := scop[0]
			bc := redis.BitCount{
				Start: startbit,
				End:   endbit,
			}
			res, err := bm.Client.BitCount(ctx, bm.Key, &bc).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	case 2:
		{
			startbit := scop[0]
			endbit := scop[1]
			bc := redis.BitCount{
				Start: startbit,
				End:   endbit,
			}
			res, err := bm.Client.BitCount(ctx, bm.Key, &bc).Result()
			if err != nil {
				return 0, err
			}
			return res, nil
		}
	default:
		{
			return -1, exception.ErrParamScopLengthMoreThan2
		}
	}
}

//Len 检查bitmap中被置1的有多少位
//规定报错时返回的第一位是-1
//如果key设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (bm *Bitmap) Len(ctx context.Context) (int64, error) {
	return bm.ScopCount(ctx)
}

//Reset 重置当前bitmap
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (bm *Bitmap) Reset(ctx context.Context) error {
	err := bm.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

//ToArray 检查哪些偏移量是已经被置1的
//如果设置了MaxTTL则会在执行好后刷新TTL
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (bm *Bitmap) ToArray(ctx context.Context) ([]int64, error) {
	if bm.Opt.MaxTTL != 0 {
		defer bm.RefreshTTL(ctx)
	}
	res := []int64{}
	bitmapstring, err := bm.Client.Get(ctx, bm.Key).Result()
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

//Intersection 对应set的求交集操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params targetbmkey *clientkey.ClientKey 目标key对象
//@params  otherbms ...*Bitmap 与之做交操作的其他bitmap对象
func (bm *Bitmap) Intersection(ctx context.Context, targetbmkey *clientkey.ClientKey, otherbms ...*Bitmap) (*Bitmap, error) {
	keys := []string{bm.Key}
	for _, otherbm := range otherbms {
		keys = append(keys, otherbm.Key)
	}
	_, err := bm.Client.BitOpAnd(ctx, targetbmkey.Key, keys...).Result()
	if err != nil {
		return nil, err
	}
	return New(targetbmkey), nil
}

//Union 对应set的求并集操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params targetbmkey *clientkey.ClientKey 目标key对象
//@params  otherbms ...*Bitmap 与之做并操作的其他bitmap对象
func (bm *Bitmap) Union(ctx context.Context, targetbmkey *clientkey.ClientKey, otherbms ...*Bitmap) (*Bitmap, error) {
	keys := []string{bm.Key}
	for _, otherbm := range otherbms {
		keys = append(keys, otherbm.Key)
	}
	_, err := bm.Client.BitOpOr(ctx, targetbmkey.Key, keys...).Result()
	if err != nil {
		return nil, err
	}
	return New(targetbmkey), nil
}

//Xor 对应set的求对称差集操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params targetbmkey *clientkey.ClientKey 目标key对象
//@params  otherbm *Bitmap 与之做xor操作的其他bitmap对象
func (bm *Bitmap) Xor(ctx context.Context, targetbmkey *clientkey.ClientKey, otherbm *Bitmap) (*Bitmap, error) {
	keys := []string{bm.Key, otherbm.Key}

	_, err := bm.Client.BitOpXor(ctx, targetbmkey.Key, keys...).Result()
	if err != nil {
		return nil, err
	}
	return New(targetbmkey), nil
}

//Except 对应set的求差集操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params targetbmkey *clientkey.ClientKey 目标key对象
//@params  otherbm *Bitmap 与之做xor操作的其他bitmap对象
func (bm *Bitmap) Except(ctx context.Context, targetbmkey *clientkey.ClientKey, otherbm *Bitmap) (*Bitmap, error) {
	keys1 := []string{bm.Key, otherbm.Key}
	u2 := uuid.NewV4()
	tempid := hex.EncodeToString(u2.Bytes())
	tempkey := fmt.Sprintf("temp::%s", tempid)
	_, err := bm.Client.BitOpXor(ctx, tempkey, keys1...).Result()
	if err != nil {
		return nil, err
	}
	keys2 := []string{tempkey, bm.Key}
	_, err = bm.Client.BitOpAnd(ctx, targetbmkey.Key, keys2...).Result()
	if err != nil {
		return nil, err
	}
	_, err = bm.Client.Del(ctx, tempkey).Result()
	if err != nil {
		return nil, err
	}
	return New(targetbmkey), nil
}
