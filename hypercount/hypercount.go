//Package hypercount 面向对象的计数估计类型
//HyperCount 用于粗略统计大量数据去重后的个数,一般用在日活计算
package hypercount

import (
	"context"

	"github.com/Golang-Tools/optparams"

	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//HyperCount 估计计数对象
type HyperCount struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的计数对象
//@params k *key.Key redis客户端的键对象
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*HyperCount, error) {
	bm := new(HyperCount)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	m, err := middlewarehelper.New(cli, "hypercounter", bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = m
	return bm, nil
}

//写操作

//AddM 添加数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params items ...interface{} 添加的数据
func (c *HyperCount) AddM(ctx context.Context, items ...interface{}) error {
	_, err := c.Client().PFAdd(ctx, c.Key(), items...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Add 添加数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params item interface{} 添加的数据
func (c *HyperCount) Add(ctx context.Context, item interface{}) error {
	_, err := c.Client().PFAdd(ctx, c.Key(), item).Result()
	if err != nil {
		return err
	}
	return nil
}

// 读操作

//Len 检查HyperLogLog中不重复元素的个数
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (c *HyperCount) Len(ctx context.Context) (int64, error) {
	return c.Client().PFCount(ctx, c.Key()).Result()
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
//@params  otherhc []*HyperCount 与之做并操作的其他HyperCount对象
func (c *HyperCount) Union(ctx context.Context, otherhc []*HyperCount, newkeyopts ...optparams.Option[Options]) (*HyperCount, error) {
	newhc, err := New(c.Client(), newkeyopts...)
	if err != nil {
		return nil, err
	}
	ukeys := []string{c.Key()}
	for _, otherkey := range otherhc {
		ukeys = append(ukeys, otherkey.Key())
	}

	_, err = c.Client().PFMerge(ctx, newhc.Key(), ukeys...).Result()
	if err != nil {
		return nil, err
	}
	return newhc, nil
}
