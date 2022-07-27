//redisbloomhelper redis扩展[RedisBloom](https://github.com/RedisBloom/RedisBloom)帮助模块,用于处理布隆过滤器
package bloomfilterhelper

import (
	"context"
	"errors"
	"fmt"

	"time"

	// log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//BloomFilter 布隆过滤器对象
//布隆过滤器常用于去重场景,原理是使用多个hash函数将内容散列至对应的较短的bitmap中,检查时只要所有bitmap中对应位置都没有它即认为未出现,反之则标明已经出现过
//布隆过滤器有碰撞概率,即并不能保证一定没有重复,而是根据设置的hash函数数量和bitmap长短可以控制碰撞概率的高低.因此也只适合用于并不严格的去重场景,但它可以大大的节省空间.
//而多数现实场景下,相比较起实打实的花钱买空间尤其是内存空间来说,一个较小的碰撞率往往是可以接受的
type BloomFilter struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的令牌桶对象
//@params cli redis.UniversalClient
//@params opts ...optparams.Option[Options] 设置对象的可选参数
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*BloomFilter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := exthelper.CheckModule(cli, ctx, "bf")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module bf yet")
	}
	bm := new(BloomFilter)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	mi, err := middlewarehelper.New(cli, bm.opt.Middlewaretype, bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = mi
	return bm, nil
}

//AddItem 布隆过器中设置物品
//@params item string 被设置的物品
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns bool 设置的物品是否已经存在
func (c *BloomFilter) AddItem(ctx context.Context, item string, opts ...optparams.Option[AddOpts]) (bool, error) {
	defaultopt := AddOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var existsint int64
	var ok bool
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 {
		ttl = refreshopt.TTL
	} else {
		if c.MaxTTL() > 0 {
			ttl = c.MaxTTL()
		}
	}
	cmd := "BF.ADD"
	if ttl > 0 && refreshopt.RefreshTTL != middlewarehelper.RefreshTTLType_NoRefresh {
		if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
			res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, item)
			if err != nil {
				return false, err
			}
			existsint, ok = res.(int64)
		} else {
			exists, err := c.MiddleWareAbc.Exists(ctx)
			if err != nil {
				return false, err
			}
			var res interface{}
			if exists {
				res, err = c.Client().Do(ctx, cmd, c.Key(), item).Result()
			} else {
				res, err = c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, item)
			}
			if err != nil {
				return false, err
			}
			existsint, ok = res.(int64)
		}
	} else {
		res, err := c.Client().Do(ctx, cmd, c.Key(), item).Result()
		if err != nil {
			return false, err
		}
		existsint, ok = res.(int64)
	}
	if !ok {
		return false, errors.New("cannot parser BF.ADD results to int64")
	}
	if existsint == 1 {
		return false, nil
	}
	return true, nil
}

func mAddItemCmds(key string, items []string) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "BF.MADD"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, i := range items {
		params = append(params, i)
		cmdswithoutttl = append(cmdswithoutttl, i)
	}
	return
}

//MAddItem 布隆过器中设置多个物品
//@params items []string
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns map[string]bool 设置的物品是否在设置前已经存在
func (c *BloomFilter) MAddItem(ctx context.Context, items []string, opts ...optparams.Option[AddOpts]) (map[string]bool, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := AddOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 {
		ttl = refreshopt.TTL
	} else {
		if c.MaxTTL() > 0 {
			ttl = c.MaxTTL()
		}
	}
	cmd, params, cmdswithoutttl := mAddItemCmds(c.Key(), items)
	var infos []interface{}
	var ok bool
	if ttl > 0 && refreshopt.RefreshTTL != middlewarehelper.RefreshTTLType_NoRefresh {
		if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
			res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
			if err != nil {
				return nil, err
			}
			infos, ok = res.([]interface{})
		} else {
			exists, err := c.MiddleWareAbc.Exists(ctx)
			if err != nil {
				return nil, err
			}
			var res interface{}
			if exists {

				res, err = c.Client().Do(ctx, cmdswithoutttl...).Result()
			} else {
				res, err = c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
			}
			if err != nil {
				return nil, err
			}
			infos, ok = res.([]interface{})
		}
	} else {
		res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	}
	if !ok {
		return nil, errors.New("cannot parser BF.MADD results to []interface{}")
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, errors.New("cannot parser BF.MADD result to int64")
		}
		if existsint == 1 {
			result[item] = false
		} else {
			result[item] = true
		}
	}
	return result, nil
}

//ExistsItem 布隆过器中检查是否已经存在
//@params item string
//@return bool 物品是否已经存在
func (c *BloomFilter) ExistsItem(ctx context.Context, item string) (bool, error) {
	var existsint int64
	var ok bool
	res, err := c.Client().Do(ctx, "BF.EXISTS", c.Key(), item).Result()
	if err != nil {
		return false, err
	}
	existsint, ok = res.(int64)
	if !ok {
		return false, errors.New("cannot parser BF.EXISTS results to int64")
	}
	if existsint == 1 {
		return true, nil
	}
	return false, nil
}

func mExistsItemCmds(key string, items ...string) (cmd string, cmdswithoutttl []interface{}) {
	cmd = "BF.MEXISTS"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, i := range items {
		cmdswithoutttl = append(cmdswithoutttl, i)
	}
	return
}

//MExistsItem 布隆过器中检查复数物品是否已经存在
//@params items ...string 待检查物品
//@return  map[string]bool 检查的物品是否已经存在
func (c *BloomFilter) MExistsItem(ctx context.Context, items ...string) (map[string]bool, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mExistsItemCmds(c.Key(), items...)
	var infos []interface{}
	var ok bool
	res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok = res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", cmd)
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, errors.New("cannot parser BF.MADD result to int64")
		}
		if existsint == 1 {
			result[item] = true
		} else {
			result[item] = false
		}
	}
	return result, nil
}

//BloomFilterInfo 布隆过滤器状态信息
type BloomFilterInfo struct {
	Capacity              int64 `json:"Capacity"`
	Size                  int64 `json:"Size"`
	NumberOfFilters       int64 `json:"NumberOfFilters"`
	NumberOfItemsInserted int64 `json:"NumberOfItemsInserted"`
	ExpansionRate         int64 `json:"ExpansionRate"`
}

//Info 查看指定bloomfilter的状态
func (c *BloomFilter) Info(ctx context.Context) (*BloomFilterInfo, error) {
	cmd := []interface{}{"BF.INFO", c.Key()}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if len(infos) != 10 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, errors.New("cannot parser BF.INFO result to []interface{}")
	}
	// log.Info("get info", log.Dict{"res": res})
	capacity, ok := infos[1].(int64)
	if !ok {
		return nil, errors.New("cannot parser Capacity result to int64")
	}
	size, ok := infos[3].(int64)
	if !ok {
		return nil, errors.New("cannot parser Size result to int64")
	}
	numberOfFilters, ok := infos[5].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfFilters result to int64")
	}
	numberOfItemsInserted, ok := infos[7].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfItemsInserted result to int64")
	}
	var expansionRate int64
	if infos[9] == nil {
		expansionRate = 0
	} else {
		expansionRate, ok = infos[9].(int64)
		if !ok {
			// log.Info("get infos", log.Dict{"infos": infos})
			return nil, errors.New("cannot parser ExpansionRate result to int64")
		}
	}

	info := BloomFilterInfo{
		Capacity:              capacity,
		Size:                  size,
		NumberOfFilters:       numberOfFilters,
		NumberOfItemsInserted: numberOfItemsInserted,
		ExpansionRate:         expansionRate,
	}
	return &info, nil
}

func mReserveCmds(key string, capacity int64, error_rate float64, opt *ReserveOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "BF.RESERVE"
	params = append(params, error_rate, capacity)
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, error_rate, capacity)
	if opt.NonScaling {
		params = append(params, "NONSCALING")
		cmdswithoutttl = append(cmdswithoutttl, "NONSCALING")
	} else {
		if opt.Expansion > 0 {
			params = append(params, "EXPANSION", opt.Expansion)
			cmdswithoutttl = append(cmdswithoutttl, "EXPANSION", opt.Expansion)
		}
	}
	return
}

//Reserve 创建一个布隆过滤器,如果有设置maxttl,则会同时为其设置一个过期
//@params capacity int64 容量,预估物品的数量,容量越大检索效率越低,但如果超出容量则会默认使用子过滤器扩容,这对检索效率的影响更大
//@params error_rate float64 碰撞率,碰撞率设置的越低使用的hash函数越多,使用的空间也越大,检索效率也越低
//@params opts ...optparams.Option[ReserveOpts] 可选设置项
func (c *BloomFilter) Reserve(ctx context.Context, capacity int64, error_rate float64, opts ...optparams.Option[ReserveOpts]) error {
	defaultopt := ReserveOpts{}
	optparams.GetOption(&defaultopt, opts...)
	cmd, params, cmdswithoutttl := mReserveCmds(c.Key(), capacity, error_rate, &defaultopt)

	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		if refreshopt.TTL > 0 {
			ttl = refreshopt.TTL
		} else {
			if c.MaxTTL() > 0 {
				ttl = c.MaxTTL()
			}
		}
	}
	if ttl > 0 {
		_, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
		if err != nil {
			return err
		}
	} else {
		_, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func mInsertCmds(key string, items []string, opt *InsertOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "BF.INSERT"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	if opt.NoCreate {
		params = append(params, "NOCREATE")
		cmdswithoutttl = append(cmdswithoutttl, "NOCREATE")
	} else {
		if opt.ErrorRate > 0 {
			params = append(params, "ERROR", opt.ErrorRate)
			cmdswithoutttl = append(cmdswithoutttl, "ERROR", opt.ErrorRate)
		}
		if opt.Capacity > 0 {
			params = append(params, "CAPACITY", opt.Capacity)
			cmdswithoutttl = append(cmdswithoutttl, "CAPACITY", opt.Capacity)
		}
		if opt.NonScaling {
			params = append(params, "NONSCALING")
			cmdswithoutttl = append(cmdswithoutttl, "NONSCALING")
		} else {
			if opt.Expansion > 0 {
				params = append(params, "EXPANSION", opt.Expansion)
				cmdswithoutttl = append(cmdswithoutttl, "EXPANSION", opt.Expansion)
			}
		}
	}
	params = append(params, "ITEMS")
	cmdswithoutttl = append(cmdswithoutttl, "ITEMS")
	for _, item := range items {
		params = append(params, item)
		cmdswithoutttl = append(cmdswithoutttl, item)
	}
	return
}

//Insert 向布隆过滤器中插入数据,如果不存在就创建
//@params items []string 待插入数据
//@params opts ...optparams.Option[InsertOpts] 可选设置项
func (c *BloomFilter) Insert(ctx context.Context, items []string, opts ...optparams.Option[InsertOpts]) (map[string]bool, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := InsertOpts{}
	optparams.GetOption(&defaultopt, opts...)

	cmd, params, cmdswithoutttl := mInsertCmds(c.Key(), items, &defaultopt)

	exists, err := c.MiddleWareAbc.Exists(ctx)
	if err != nil {
		return nil, err
	}
	var infos []interface{}
	var ok bool
	//如果原本不存在且有设置MaxTTL,则设置超时

	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 {
		ttl = refreshopt.TTL
	} else {
		if c.MaxTTL() > 0 {
			ttl = c.MaxTTL()
		}
	}
	if ttl > 0 && refreshopt.RefreshTTL != middlewarehelper.RefreshTTLType_NoRefresh {

		if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
			res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
			if err != nil {
				return nil, err
			}
			infos, ok = res.([]interface{})
		} else {
			if exists {
				res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
				if err != nil {
					return nil, err
				}
				infos, ok = res.([]interface{})
			} else {
				res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
				if err != nil {
					return nil, err
				}
				infos, ok = res.([]interface{})
			}
		}
	} else {
		res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	}
	if !ok {
		return nil, errors.New("cannot parser BF.INSERT results to []interface{}")
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, errors.New("cannot parser BF.INSERT result to int64")
		}
		if existsint == 1 {
			result[item] = false
		} else {
			result[item] = true
		}
	}
	return result, nil
}

//Clean 清除bloomfilter的key
func (c *BloomFilter) Clean(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}
