package countminsketch

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

//CountMinSketch count-min sketch对象
//(Count–min sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)算法是一个在数据大小非常大时使用的一种通过牺牲准确性提高的效率的计数算法.
//这个算法的原理是:不存储所有的不同的元素,只存储它们Sketch的计数.
//基本的思路如下:
//1. 创建n个长度为 X 的数组A用来计数(i属于n,第i个数组的长度即为x_i,第i个数组即为A_i),初始化每个元素的计数值为 0
//2. 对于一个新来的元素y,用n个hash函数分别计算hash_i(y)到 0 到 x_i 之间的一个数作为数组的位置索引J(第i个数组的索引即为J_i)
//3. 数组A_i对应的索引位置的计数值加一(A_i[J_i] += 1)
//4. 这时要查询某个元素出现的频率，只要简单的返回这个元素哈希望后对应的数组的位置索引的计数值的最小值即可.
//在实践中我们会控制数组的长度固定,数组的个数和数组的长度就是这个算法初始化的变量了,其中`width`是数组的长度,控制误差大小,`depth`是数组的宽度,控制膨胀计数的期望概率,也会影响内存占用
//注意,和两个filter不同,cms必须先创建,incr不会创建cms结构,可以先创建再调用init方法初始化,也可以创建时使用对应的可选参数同步初始化
type CountMinSketch struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个新的令牌桶对象
//@params cli redis.UniversalClient
//@params opts ...optparams.Option[Options] 设置对象的可选参数,注意 WithInitProbability 和 WithInitDIM如果有填只会取第一个
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*CountMinSketch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := exthelper.CheckModule(cli, ctx, "bf")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module bf yet")
	}
	bm := new(CountMinSketch)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	mi, err := middlewarehelper.New(cli, bm.opt.Middlewaretype, bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = mi
	if bm.MaxTTL() > 0 {
		bm.opt.InitOpts = append(bm.opt.InitOpts, InitWithRefreshTTL())
	}
	if len(bm.opt.InitOpts) > 0 {
		err := bm.Init(ctx, bm.opt.InitOpts...)
		if err != nil {
			return nil, err
		}
	}
	return bm, nil
}
func initCmds(key string, opt *InitOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}, err error) {
	cmd = "CMS.INITBYDIM"
	if opt.Depth > 0 && opt.Width > 0 {
		params = append(params, opt.Width, opt.Depth)
		cmdswithoutttl = append(cmdswithoutttl, cmd, key, opt.Width, opt.Depth)
	} else {
		if opt.ErrorRate > 0 && opt.Probability > 0 {
			cmd = "CMS.INITBYPROB"
			params = append(params, opt.ErrorRate, opt.Probability)
			cmdswithoutttl = append(cmdswithoutttl, cmd, key, opt.ErrorRate, opt.Probability)
		} else {
			err = redisbloomhelper.ErrMustChooseOneParam
		}
	}
	return
}

//Init 初始化CountMinSketch
//@params opts ...optparams.Option[InitOpts] 注意InitWithProbability和InitWithDIM必选一个,如果选了超过一个则会默认取第一个
func (c *CountMinSketch) Init(ctx context.Context, opts ...optparams.Option[InitOpts]) error {
	if len(opts) < 1 {
		return redisbloomhelper.ErrMustChooseOneParam
	}
	defaultopt := InitOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl, err := initCmds(c.Key(), &defaultopt)
	if err != nil {
		return err
	}
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
		_, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), c.MaxTTL(), params...)
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

func mIncrItemCmds(key string, opt *IncrOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}, items []string) {
	cmd = "CMS.INCRBY"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for index, item := range opt.MincrItems {
		incr := opt.MincrIncrement[index]
		params = append(params, item, incr)
		cmdswithoutttl = append(cmdswithoutttl, item, incr)
		items = append(items, item)
	}
	return
}

//MIncrItem 为物品添加计数
//@params  opts ...optparams.Option[IncrOpts] 增加数据时的设置,请使用`IncrWithItemMap`或`IncrWithIncrItems`设置物品及其增量
//@returns map[string]int64 物品在新增后的存量
func (c *CountMinSketch) MIncrItem(ctx context.Context, opts ...optparams.Option[IncrOpts]) (map[string]int64, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
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
	itemlen := len(defaultopt.MincrItems)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, params, cmdswithoutttl, items := mIncrItemCmds(c.Key(), &defaultopt)
	var infos []interface{}
	var ok bool
	if ttl > 0 {
		// log.Debug("RefreshTTLType_Refresh")
		res, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), ttl, params...)
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	} else {
		res, err := c.Client().Do(ctx, cmdswithoutttl...).Result()
		if err != nil {
			return nil, err
		}
		infos, ok = res.([]interface{})
	}
	if !ok {
		return nil, errors.New("cannot parser CMS.INCRBY results to []interface{}")
	}
	if len(infos) != itemlen {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	result := map[string]int64{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, errors.New("cannot parser CMS.INCRBY result to int64")
		}
		result[item] = existsint
	}
	return result, nil
}

//IncrItem 为物品添加计数
//@params item string 要增加的物品
//@params opts ...optparams.Option[IncrOpts] 增加数据时的设置,可以使用`IncrWithIncrement`额外指定物品的增量
//@returns int64 物品在新增后的存量
func (c *CountMinSketch) IncrItem(ctx context.Context, item string, opts ...optparams.Option[IncrOpts]) (int64, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
	newopt := []optparams.Option[IncrOpts]{}
	if defaultopt.Increment > 0 {
		newopt = append(newopt, IncrWithIncrItems(&redisbloomhelper.IncrItem{Item: item, Increment: defaultopt.Increment}))
	} else {
		newopt = append(newopt, IncrWithIncrItems(&redisbloomhelper.IncrItem{Item: item, Increment: 1}))
	}
	if len(defaultopt.RefreshOpts) > 0 {
		newopt = append(newopt, mIncr(defaultopt.RefreshOpts...))
	}
	ress, err := c.MIncrItem(ctx, newopt...)
	if err != nil {
		return 0, err
	}
	return ress[item], nil
}

func mQueryItemCmds(key string, items ...string) (cmd string, cmdswithoutttl []interface{}) {
	cmd = "CMS.QUERY"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, item := range items {
		cmdswithoutttl = append(cmdswithoutttl, item)
	}
	return
}

//MQueryItem 查看物品当前计数
//@params  items ...string 待查看物品
//@returns map[string]int64 物品在新增后的存量
func (c *CountMinSketch) MQueryItem(ctx context.Context, items ...string) (map[string]int64, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mQueryItemCmds(c.Key(), items...)
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
	result := map[string]int64{}
	for index, item := range items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, fmt.Errorf("cannot parser %s result to int64", cmd)
		}
		result[item] = existsint
	}
	return result, nil
}

//QeryItem 查看物品当前计数
//@params  item string 代查看物品
//@returns int64 物品在新增后的存量
func (c *CountMinSketch) QueryItem(ctx context.Context, item string) (int64, error) {
	ress, err := c.MQueryItem(ctx, item)
	if err != nil {
		return 0, err
	}
	return ress[item], nil
}

//CountMinSketchInfo CountMinSketch数据结构状态信息
type CountMinSketchInfo struct {
	Width int64 `json:"Width"`
	Depth int64 `json:"Depth"`
	Count int64 `json:"Count"`
}

//Info 查看指定CountMinSketch的状态
func (c *CountMinSketch) Info(ctx context.Context) (*CountMinSketchInfo, error) {
	cmd := []interface{}{"CMS.INFO", c.Key()}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if len(infos) != 6 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, errors.New("cannot parser CMS.INFO result to []interface{}")
	}
	// log.Info("get info", log.Dict{"res": res})
	width, ok := infos[1].(int64)
	if !ok {
		return nil, errors.New("cannot parser Width result to int64")
	}
	depth, ok := infos[3].(int64)
	if !ok {
		return nil, errors.New("cannot parser Depth result to int64")
	}
	count, ok := infos[5].(int64)
	if !ok {
		return nil, errors.New("cannot parser Count result to int64")
	}
	info := CountMinSketchInfo{
		Width: width,
		Depth: depth,
		Count: count,
	}
	return &info, nil
}

type WeightedCountMinSketch struct {
	Src    *CountMinSketch
	Weight int64
}

type WeightedCountMinSketchKey struct {
	Key    string
	Weight int64
}

func mergeCmds(key string, sources ...*WeightedCountMinSketchKey) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "CMS.MERGE"
	sourcelen := len(sources)
	params = append(params, sourcelen)
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, sourcelen)
	sourcekeys := []interface{}{}
	sourceweights := []interface{}{}
	for _, source := range sources {
		sourcekeys = append(sourcekeys, source.Key)
		if source.Weight > 0 {
			sourceweights = append(sourceweights, source.Weight)
		} else {
			sourceweights = append(sourceweights, 1)
		}
	}
	params = append(params, sourcekeys...)
	cmdswithoutttl = append(cmdswithoutttl, sourcekeys...)
	params = append(params, "WEIGHTS")
	cmdswithoutttl = append(cmdswithoutttl, "WEIGHTS")
	params = append(params, sourceweights...)
	cmdswithoutttl = append(cmdswithoutttl, sourceweights...)
	return
}

//InitFromSources 将几个CountMinSketch的内容merge进自己这个key从而初始化
//@params sources ...*WeightedCountMinSketch 用于初始化的其他CountMinSketch和其设置的权重序列
func (c *CountMinSketch) InitFromSources(ctx context.Context, sources ...*WeightedCountMinSketch) error {
	sourcelen := len(sources)
	if sourcelen < 2 {
		return redisbloomhelper.ErrNeedMoreThan2Source
	}
	width := int64(0)
	depth := int64(0)
	sourcekeys := []*WeightedCountMinSketchKey{}
	for _, source := range sources {
		info, err := source.Src.Info(ctx)
		if err != nil {
			return err
		}
		if width != 0 {
			if width != info.Width {
				return errors.New("merge width not match")
			}
		} else {
			width = info.Width
		}
		if depth != 0 {
			if depth != info.Depth {
				return errors.New("merge depth not match")
			}
		} else {
			depth = info.Depth
		}
		sourcekeys = append(sourcekeys, &WeightedCountMinSketchKey{Key: source.Src.Key(), Weight: source.Weight})
	}
	info, err := c.Info(ctx)
	if err != nil {
		if err.Error() == "CMS: key does not exist" {
			err := c.Init(ctx, InitWithDIM(width, depth))
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		if info.Depth != depth {
			return errors.New("merge depth not match")
		}
		if info.Width != width {
			return errors.New("merge width not match")
		}
	}
	cmd, params, cmdswithoutttl := mergeCmds(c.Key(), sourcekeys...)
	if c.MaxTTL() > 0 {
		_, err := c.DoCmdWithTTL(ctx, cmd, c.Key(), c.MaxTTL(), params...)
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

//Merge 自己和几个其他的CountMinSketch合并构造一个新的CountMinSketch
//@params weight int64 自身保存数据在新merge出来的CountMinSketch中的权重
//@params othersources []*WeightedCountMinSketch 用于merge的其他CountMinSketch和其设置的权重序列
//@params opts ...optparams.Option[Options] 创建新CountMinSketch的设置,我们可以使用这个设置新key的命名等
//@returns *CountMinSketch 融合了自己和othersources信息的新CountMinSketch对象
func (c *CountMinSketch) Merge(ctx context.Context, weight int64, othersources []*WeightedCountMinSketch, opts ...optparams.Option[Options]) (*CountMinSketch, error) {
	sourcelen := len(othersources)
	if sourcelen < 1 {
		return nil, redisbloomhelper.ErrNeedMoreThan2Source
	}
	newcm, err := New(c.Client(), opts...)
	if err != nil {
		return nil, err
	}
	self := WeightedCountMinSketch{
		Src:    c,
		Weight: weight,
	}
	othersources = append(othersources, &self)
	err = newcm.InitFromSources(ctx, othersources...)
	if err != nil {
		return nil, err
	}
	return newcm, nil
}

//Clean 清除CountMinSketch 的key
func (c *CountMinSketch) Clean(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}
