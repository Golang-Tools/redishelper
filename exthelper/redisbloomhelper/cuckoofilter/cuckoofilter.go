//cuckoofilter redis扩展[RedisBloom](https://github.com/RedisBloom/RedisBloom)帮助模块,用于处理布隆过滤器
package cuckoofilter

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

//CuckooFilter 布谷鸟过滤器对象
//[布谷鸟过滤器](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)
//常用于去重场景,其原理来自[cuckoo hash](https://en.wikipedia.org/wiki/Cuckoo_hashing)这种算法.这种算法的特点可以总结为`鸠占鹊巢`.
//最原始的布谷鸟哈希方法是使用两个哈希函数对一个key进行哈希,得到桶中的两个位置,此时
//1. 如果两个位置都为为空则将key随机存入其中一个位置
//2. 如果只有一个位置为空则存入为空的位置
//3. 如果都不为空,则随机踢出一个元素踢出的元素再重新计算哈希找到相应的位置
//当然假如存在绝对的空间不足那老是踢出也不是办法,所以一般会设置一个踢出阈值(`MaxIteration`),如果在某次插入行为过程中连续踢出超过阈值则进行扩容
//
//布谷鸟过滤器的布谷鸟哈希表的基本单位称为条目(entry),每个条目存储一个指纹(fingerprint),指纹指的是使用一个哈希函数生成的n位比特位,n的具体大小由所能接受的误判率来设置,通常都比较小.
//哈希表通常由1个桶数组组成
// + 其中一个桶可以有多个条目,单个桶中的的条目数即为`BucketSize`.`BucketSize`的大小会对搜索效存储效率都有影响.
//		+`BucketSize`越小则假阳性率越大,表的利用率也就越低.默认的`BucketSize`为2,可以有84%的负载因子α当设置为3时可以到95%,设置为4是可以到98%
//      + BucketSize`越大就需要越长的指纹才能保持相同的假阳性率(即b越大f越大).使用较大的桶时每次查找都会检查更多的条目,从而有更大的概率产生指纹冲突.
// + 一个桶数组中包含桶的个数即为`NumberOfBuckets`,我们如果可以预估到总量的话可以使用如下公式进行设置
//      2^(n-1)< capacity < NumberOfBuckets*BucketSize = 2^n
//      在初始化时我们可以指定capacity和BucketSize,NumberOfBuckets则是用上面的关系自动分配的
//
//布谷鸟过滤器一般采用2个桶数组,同时采取了两个并不独立的哈希函数(即下面的hash和𝑓𝑖𝑛𝑔𝑒𝑟𝑝𝑟𝑖𝑛𝑡)计算一个物品的索引和指纹:
//  i_1=ℎ𝑎𝑠ℎ(𝑥)
//  𝑓=𝑓𝑖𝑛𝑔𝑒𝑟𝑝𝑟𝑖𝑛𝑡(𝑥)
//  i_2=i_1 xor ℎ𝑎𝑠ℎ(𝑓)
//i_1和i_2 即计算出来哈希表中两个桶所在的索引,这样我们可以得到一个坐标即(i_1,i_2)和一个数据的指纹(f),插入时则使用上面布谷鸟哈希方法插入;查询则是通过这个坐标取出两个桶中的数据匹配是否存在指纹.
//如果要删除一个已经存在的物品也是一样,确定好坐标和指纹后在桶中找出匹配的条目清空即可.不过也要注意删除未被添加的物品可能会引起被添加的物品被删除,因此需要谨慎使用
//
//布谷过滤器在错误率要求小于3%的场景下空间性能优于布隆过滤器(实际应用场景下常常满足),而且支持动态删除物品.
//而且布谷鸟过滤器按照普通设计只有两个桶,相比于布隆过滤器的多个Hash函数多次访存在数据量很大不能全部装载在内存中的情况下往往可以提供更好的访问效率.
//布谷过滤器也有其相应的缺点:
// 1. 当装填因子较高的时候容易出现循环的问题--即插入失败的情况.
// 2. 就是访问空间地址不连续,通常可以认为是随机的,这个问题在布隆过滤器中也是一样.这样严重破坏了程序局部性,对于Cache流水线来说非常不利.
type Cuckoofilter struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的令牌桶对象
//@params cli redis.UniversalClient
//@params opts ...optparams.Option[Options] 设置对象的可选参数
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Cuckoofilter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := exthelper.CheckModule(cli, ctx, "bf")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module bf yet")
	}
	bm := new(Cuckoofilter)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	mi, err := middlewarehelper.New(cli, bm.opt.Middlewaretype, bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = mi
	return bm, nil
}

func addItemCmds(key string, item string, opt *AddOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "CF.ADD"
	if opt.NX {
		cmd = "CF.ADDNX"
	}
	params = append(params, item)
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, item)
	return
}

//AddItem cuckoofilter中设置物品,
//注意如果不设置NX则会重复向表中插入数据,返回全部为false,尽量带nx插入
//@params item string 要插入的物品
//@params opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns bool 设置的物品是否已经存在,true表示已经存在,不会插入
func (c *Cuckoofilter) AddItem(ctx context.Context, item string, opts ...optparams.Option[AddOpts]) (bool, error) {
	defaultopt := AddOpts{
		NX: true,
	}
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
	cmd, params, cmdswithoutttl := addItemCmds(c.Key(), item, &defaultopt)
	if ttl > 0 && refreshopt.RefreshTTL != middlewarehelper.RefreshTTLType_NoRefresh {
		if refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
			res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
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
				res, err = c.Client().Do(ctx, cmdswithoutttl...).Result()
			} else {
				res, err = c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
			}
			if err != nil {
				return false, err
			}
			existsint, ok = res.(int64)
		}
	} else {
		res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
		if err != nil {
			return false, err
		}
		existsint, ok = res.(int64)
	}
	if !ok {
		return false, fmt.Errorf("cannot parser %s results to int64", cmd)
	}
	if existsint == 1 {
		return false, nil
	}
	return true, nil
}

func mAddItemCmds(key string, items []string, opt *AddOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "CF.INSERT"
	if opt.NX {
		cmd = "CF.INSERTNX"
	}
	params = append(params, "ITEMS")
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, "ITEMS")
	for _, i := range items {
		params = append(params, i)
		cmdswithoutttl = append(cmdswithoutttl, i)
	}
	return
}

//MAddItem cuckoofilter中设置多个物品,使用的是INSERT或者INSERTNX命令
//注意如果不设置NX则会重复向表中插入数据,返回全部为false,尽量带nx插入
//@params item []string 要插入的物品
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns map[string]bool 设置的物品是否在设置前已经存在,true表示已经存在,并未插入
func (c *Cuckoofilter) MAddItem(ctx context.Context, items []string, opts ...optparams.Option[AddOpts]) (map[string]bool, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := AddOpts{
		NX: true,
	}
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
	cmd, params, cmdswithoutttl := mAddItemCmds(c.Key(), items, &defaultopt)
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
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", cmd)
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, fmt.Errorf("cannot parser %s result to int64", cmd)
		}
		if existsint == 1 {
			result[item] = false
		} else {
			result[item] = true
		}
	}
	return result, nil
}

//ExistsItem cuckoofilter中检查是否已经存在
//@params item string 待检查的物品
//@return bool 物品是否已经存在
func (c *Cuckoofilter) ExistsItem(ctx context.Context, item string) (bool, error) {
	var existsint int64
	var ok bool
	res, err := c.Client().Do(ctx, "CF.EXISTS", c.Key(), item).Result()
	if err != nil {
		return false, err
	}
	existsint, ok = res.(int64)
	if !ok {
		return false, errors.New("cannot parser CF.EXISTS results to int64")
	}
	if existsint == 1 {
		return true, nil
	}
	return false, nil
}

func mExistsItemCmds(key string, items ...string) (cmd string, cmdswithoutttl []interface{}) {
	cmd = "CF.MEXISTS"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, i := range items {
		cmdswithoutttl = append(cmdswithoutttl, i)
	}
	return
}

//MExistsItem cuckoofilter中检查复数物品是否已经存在
//@params item ...string 待检查的物品
//@return  map[string]bool 检查的物品是否已经存在
func (c *Cuckoofilter) MExistsItem(ctx context.Context, items ...string) (map[string]bool, error) {
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
			return nil, fmt.Errorf("cannot parser %s result to int64", cmd)
		}
		if existsint == 1 {
			result[item] = true
		} else {
			result[item] = false
		}
	}
	return result, nil
}

//CuckoofilterInfo cuckoofilter状态信息
type CuckoofilterInfo struct {
	Size                  int64 `json:"Size"`
	NumberOfBuckets       int64 `json:"NumberOfBuckets"`
	NumberOfFilter        int64 `json:"NumberOfFilter"`
	NumberOfItemsInserted int64 `json:"NumberOfItemsInserted"`
	NumberOfItemsDeleted  int64 `json:"NumberOfItemsDeleted"`
	BucketSize            int64 `json:"BucketSize"`
	ExpansionRate         int64 `json:"ExpansionRate"`
	MaxIteration          int64 `json:"MaxIteration"`
}

//Info 查看指定cuckoofilter的状态
func (c *Cuckoofilter) Info(ctx context.Context) (*CuckoofilterInfo, error) {
	cmd := []interface{}{"CF.INFO", c.Key()}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if len(infos) != 16 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, errors.New("cannot parser CF.INFO result to []interface{}")
	}
	size, ok := infos[1].(int64)
	if !ok {
		return nil, errors.New("cannot parser Size result to int64")
	}
	numberOfBuckets, ok := infos[3].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfBuckets result to int64")
	}
	numberOfFilter, ok := infos[5].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfFilter result to int64")
	}
	numberOfItemsInserted, ok := infos[7].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfItemsInserted result to int64")
	}
	numberOfItemsDeleted, ok := infos[9].(int64)
	if !ok {
		return nil, errors.New("cannot parser NumberOfItemsDeleted result to int64")
	}

	bucketSize, ok := infos[11].(int64)
	if !ok {
		return nil, errors.New("cannot parser BucketSize result to int64")
	}
	expansionRate, ok := infos[13].(int64)
	if !ok {
		return nil, errors.New("cannot parser ExpansionRate result to int64")
	}
	maxIteration, ok := infos[15].(int64)
	if !ok {
		return nil, errors.New("cannot parser MaxIteration result to int64")
	}
	info := CuckoofilterInfo{
		Size:                  size,
		NumberOfBuckets:       numberOfBuckets,
		NumberOfFilter:        numberOfFilter,
		NumberOfItemsInserted: numberOfItemsInserted,
		NumberOfItemsDeleted:  numberOfItemsDeleted,
		BucketSize:            bucketSize,
		ExpansionRate:         expansionRate,
		MaxIteration:          maxIteration,
	}
	return &info, nil
}

func reserveCmds(key string, capacity int64, opt *ReserveOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "CF.RESERVE"
	params = append(params, capacity)
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, capacity)

	if opt.BucketSize > 0 {
		params = append(params, "BUCKETSIZE", opt.BucketSize)
		cmdswithoutttl = append(cmdswithoutttl, "BUCKETSIZE", opt.BucketSize)
	}
	if opt.MaxIterations > 0 {
		params = append(params, "MAXITERATIONS", opt.MaxIterations)
		cmdswithoutttl = append(cmdswithoutttl, "MAXITERATIONS", opt.MaxIterations)
	}
	if opt.Expansion > 0 {
		params = append(params, "EXPANSION", opt.Expansion)
		cmdswithoutttl = append(cmdswithoutttl, "EXPANSION", opt.Expansion)
	}
	return
}

//Reserve 创建一个cuckoofilter,如果有设置maxttl,则会同时为其设置一个过期
//@params capacity int64 容量,预估物品的数量,容量越大检索效率越低,但如果超出容量则会默认使用子过滤器扩容,这对检索效率的影响更大
//@params opts ...optparams.Option[ReserveOpts] 可选设置项
func (c *Cuckoofilter) Reserve(ctx context.Context, capacity int64, opts ...optparams.Option[ReserveOpts]) error {
	defaultopt := ReserveOpts{}
	optparams.GetOption(&defaultopt, opts...)
	cmd, params, cmdswithoutttl := reserveCmds(c.Key(), capacity, &defaultopt)
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

func insertCmds(key string, items []string, opt *InsertOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "CF.INSERT"
	if opt.NX {
		cmd = "CF.INSERTNX"
	}
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	if opt.NoCreate {
		params = append(params, "NOCREATE")
		cmdswithoutttl = append(cmdswithoutttl, "NOCREATE")
	} else {
		if opt.Capacity > 0 {
			params = append(params, "CAPACITY", opt.Capacity)
			cmdswithoutttl = append(cmdswithoutttl, "CAPACITY", opt.Capacity)
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

//Insert 向一个cuckoofilter中插入数据,如果不存在则创建
//@params items []string 待插入物品
//@params opts ...optparams.Option[InsertOpts] 可选设置项
func (c *Cuckoofilter) Insert(ctx context.Context, items []string, opts ...optparams.Option[InsertOpts]) (map[string]bool, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := InsertOpts{
		NX: true,
	}
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
	cmd, params, cmdswithoutttl := insertCmds(c.Key(), items, &defaultopt)
	exists, err := c.MiddleWareAbc.Exists(ctx)
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", cmd)
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, fmt.Errorf("cannot parser %s result to int64", cmd)
		}
		if existsint == 1 {
			result[item] = false
		} else {
			result[item] = true
		}
	}
	return result, nil
}

//CountItem cuckoofilter中检查item存在的个数
//@params item string
//@return int64 计数item的个数,如果报错则返回0
func (c *Cuckoofilter) CountItem(ctx context.Context, item string) (int64, error) {
	var existsint int64
	var ok bool
	res, err := c.Client().Do(ctx, "CF.COUNT", c.Key(), item).Result()
	if err != nil {
		return 0, err
	}
	existsint, ok = res.(int64)
	if !ok {
		return 0, errors.New("cannot parser CF.COUNT results to int64")
	}
	return existsint, nil
}

//DelItem cuckoofilter中检查item存在的个数
//@params item string
//@return bool item是否删除成功,true为删除成功,false表示不存在item无法删除
func (c *Cuckoofilter) DelItem(ctx context.Context, item string) (bool, error) {
	var existsint int64
	var ok bool
	res, err := c.Client().Do(ctx, "CF.DEL", c.Key(), item).Result()
	if err != nil {
		return false, err
	}
	existsint, ok = res.(int64)
	if !ok {
		return false, errors.New("cannot parser CF.DEL results to int64")
	}
	if existsint == 1 {
		return true, nil
	}
	return false, nil
}

//Clean 清除cuckoofilter的key
func (c *Cuckoofilter) Clean(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}
