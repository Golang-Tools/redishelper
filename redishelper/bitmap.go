package redishelper

import (
	"github.com/go-redis/redis/v7"
)

// distributedLock 分布式锁结构
type bitmap struct {
	proxy *redisHelper
	key   string
}

func newBitmap(proxy *redisHelper, key string) *bitmap {
	bm := new(bitmap)
	bm.key = key
	bm.proxy = proxy
	return bm
}

// IsSetted 查看offset位置是否已被设置
func (bm *bitmap) IsSetted(offset int64) (bool, error) {
	if !bm.proxy.IsOk() {
		return false, ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return false, err
	}
	res, err := conn.GetBit(bm.key, offset).Result()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}

// CountSetted 查看所有已经被设置为1的个数
func (bm *bitmap) CountSetted() (int64, error) {
	if !bm.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	res, err := conn.BitCount(bm.key, nil).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

// CountSetted 查看所有已经被设置为1的个数
func (bm *bitmap) CountSettedWithRange(start, end int64) (int64, error) {
	if !bm.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	opt := redis.BitCount{}
	if start != 0 {
		opt.Start = start
	}
	if end != 0 {
		opt.End = end
	}
	res, err := conn.BitCount(bm.key, &opt).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

// Set 将某位offset 设置为1,如果已经被设置了也不会报错
func (bm *bitmap) Set(offset int64) error {
	if !bm.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.SetBit(bm.key, offset, 1).Result()
	if err != nil {
		return err
	}
	return nil
}

// SetM 将多位offset 设置为1,如果已经被设置了也不会报错
func (bm *bitmap) SetM(offsets []int64) error {
	if !bm.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return err
	}
	pipe := conn.TxPipeline()
	for _, offset := range offsets {
		pipe.SetBit(bm.key, offset, 1)
	}
	_, err = pipe.Exec()
	if err != nil {
		return err
	}
	return nil
}

// UnSet 将某位offset 取消设置,即设置为0,如果本来就没有被设置也不会报错
func (bm *bitmap) UnSet(offset int64) error {
	if !bm.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.SetBit(bm.key, offset, 1).Result()
	if err != nil {
		return err
	}
	return nil
}

// UnSetM 将多位offset 设置为0,如果已经被设置了也不会报错
func (bm *bitmap) UnSetM(offsets []int64) error {
	if !bm.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return err
	}
	pipe := conn.TxPipeline()
	for _, offset := range offsets {
		pipe.SetBit(bm.key, offset, 0)
	}
	_, err = pipe.Exec()
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

func (bm *bitmap) SettedOffsets() ([]int64, error) {
	if !bm.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := bm.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	bitmapstring, err := conn.Get(bm.key).Result()
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
