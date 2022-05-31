//Package hypercount 面向对象的计数估计类型
//HyperCount 用于粗略统计大量数据去重后的个数,一般用在日活计算
package hypercount

import (
	"context"

	"github.com/Golang-Tools/redishelper/v2/clientkey"
)

//HyperCount 估计计数对象
type HyperCount struct {
	*clientkey.ClientKey
}

//New 创建一个新的计数对象
//@params k *key.Key redis客户端的键对象
func New(k *clientkey.ClientKey) *HyperCount {
	bm := new(HyperCount)
	bm.ClientKey = k
	return bm
}

//写操作

//AddM 添加数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params items ...interface{} 添加的数据
func (c *HyperCount) AddM(ctx context.Context, items ...interface{}) error {
	_, err := c.Client.PFAdd(ctx, c.Key, items...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Add 添加数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params item interface{} 添加的数据
func (c *HyperCount) Add(ctx context.Context, item interface{}) error {
	_, err := c.Client.PFAdd(ctx, c.Key, item).Result()
	if err != nil {
		return err
	}
	return nil
}

// 读操作

//Len 检查HyperLogLog中不重复元素的个数
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *HyperCount) Len(ctx context.Context) (int64, error) {
	return c.Client.PFCount(ctx, c.Key).Result()
}

//Reset 重置当前hypercount
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *HyperCount) Reset(ctx context.Context) error {
	err := c.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

//Union 对应set的求并集操作
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params targetbmkey *clientkey.ClientKey 目标key对象
//@params  otherbms ...*Bitmap 与之做并操作的其他bitmap对象
func (c *HyperCount) Union(ctx context.Context, targetbmkey *clientkey.ClientKey, otherbm *HyperCount) (*HyperCount, error) {
	keys := []string{c.Key, otherbm.Key}
	_, err := c.Client.PFMerge(ctx, targetbmkey.Key, keys...).Result()
	if err != nil {
		return nil, err
	}
	return New(targetbmkey), nil
}
