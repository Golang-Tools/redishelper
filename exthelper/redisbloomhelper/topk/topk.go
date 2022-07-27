package topk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	// log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//TopK TopK对象,用于统计大量数据下的前几位有哪些
//比较常见的场景比如大量数据情况下的排行榜应用
//使用了算法[HeavyKeeper](https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf).
//其基本原理是使用一个shape为(width,depth)的表保存物品的签名和计数,通过hash函数将签名和计数更新进表,并同时使用指数衰减的方法淘汰小流量数据
//取的时候取其中的最大值作为物品的计数
//注意,和两个filter不同,cms必须先创建,incr不会创建cms结构,可以先创建再调用init方法初始化,也可以创建时使用对应的可选参数同步初始化
type TopK struct {
	*middlewarehelper.MiddleWareAbc
	opt Options
}

//New 创建一个topk对象
//@params cli redis.UniversalClient
//@params opts ...optparams.Option[Options]
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*TopK, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := exthelper.CheckModule(cli, ctx, "bf")
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("redis server not load module bf yet")
	}
	bm := new(TopK)
	bm.opt = Defaultopt
	optparams.GetOption(&bm.opt, opts...)
	mi, err := middlewarehelper.New(cli, bm.opt.Middlewaretype, bm.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	bm.MiddleWareAbc = mi
	if bm.opt.InitTopK > 0 {
		if bm.MaxTTL() > 0 {
			bm.opt.InitOpts = append(bm.opt.InitOpts, InitWithRefreshTTL())
		}
		err := bm.Init(ctx, bm.opt.InitTopK, bm.opt.InitOpts...)
		if err != nil {
			return nil, err
		}
	}
	return bm, nil
}

func initCmds(key string, topk int64, opt *InitOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}) {
	cmd = "TOPK.RESERVE"
	params = append(params, topk)
	cmdswithoutttl = append(cmdswithoutttl, cmd, key, topk)
	if opt.Width > 0 {
		params = append(params, opt.Width)
		cmdswithoutttl = append(cmdswithoutttl, opt.Width)
	}
	if opt.Depth > 0 {
		params = append(params, opt.Depth)
		cmdswithoutttl = append(cmdswithoutttl, opt.Depth)
	}
	if opt.Decay > 0 {
		params = append(params, opt.Decay)
		cmdswithoutttl = append(cmdswithoutttl, opt.Decay)
	}
	// log.Debug("initCmds", log.Dict{"cmd": cmd, "params": params, "cmdswithoutttl": cmdswithoutttl})
	return
}

//Init 初始化TopK
//@params opts ...optparams.Option[InitOpts] 注意InitWithProbability和InitWithDIM必选一个,如果选了超过一个则会默认取第一个
func (c *TopK) Init(ctx context.Context, topk int64, opts ...optparams.Option[InitOpts]) error {
	defaultopt := InitOpts{}
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
	cmd, params, cmdswithoutttl := initCmds(c.Key(), topk, &defaultopt)
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

//Clean 清除TopK 的key
func (c *TopK) Clean(ctx context.Context) error {
	_, err := c.Client().Del(ctx, c.Key()).Result()
	if err != nil {
		return err
	}
	return nil
}

func mIncrItemCmds(key string, opt *IncrOpts) (cmd string, params []interface{}, cmdswithoutttl []interface{}, err error) {
	itemlen := len(opt.MincrItems)
	if itemlen == 0 {
		err = redisbloomhelper.ErrItemListEmpty
		return
	}
	incrlen := len(opt.MincrIncrement)
	cmd = "TOPK.INCRBY"
	if incrlen == 0 {
		cmd = "TOPK.ADD"
		cmdswithoutttl = append(cmdswithoutttl, cmd, key)
		for _, item := range opt.MincrItems {
			params = append(params, item)
			cmdswithoutttl = append(cmdswithoutttl, item)
		}
	} else {
		if incrlen != itemlen {
			err = redisbloomhelper.ErrItemLenNotMatch
			return
		}
		cmdswithoutttl = append(cmdswithoutttl, cmd, key)
		for index, item := range opt.MincrItems {
			incr := opt.MincrIncrement[index]
			params = append(params, item, incr)
			cmdswithoutttl = append(cmdswithoutttl, item, incr)
		}
	}
	return
}

//MIncrItem 为物品添加计数
//@params  opts ...optparams.Option[IncrOpts] 请求设置项,请使用`IncrWithItems`或`IncrWithItemMap`或`IncrWithIncrItems`设置物品及其增量
//@returns []string 新增后被挤出队列的物品
func (c *TopK) MIncrItem(ctx context.Context, opts ...optparams.Option[IncrOpts]) ([]string, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl, err := mIncrItemCmds(c.Key(), &defaultopt)
	if err != nil {
		return nil, err
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
	var infos []interface{}
	var ok bool
	if ttl > 0 {
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
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", cmd)
	}
	result := []string{}
	for _, expelleditem := range infos {
		if expelleditem != nil {
			item, ok := expelleditem.(string)
			if !ok {
				return nil, fmt.Errorf("cannot parser %s result to int64", cmd)
			}
			result = append(result, item)
		}
	}
	return result, nil
}

//IncrItem 为物品添加计数
//@params  item string 物品及其对应的增量
//@returns []string 新增后被挤出队列的物品
func (c *TopK) IncrItem(ctx context.Context, item string, opts ...optparams.Option[IncrOpts]) ([]string, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
	newopt := []optparams.Option[IncrOpts]{}
	if defaultopt.Increment > 0 {
		newopt = append(newopt, IncrWithIncrItems(&redisbloomhelper.IncrItem{Item: item, Increment: defaultopt.Increment}))
	} else {
		newopt = append(newopt, IncrWithItems(item))
	}
	if len(defaultopt.RefreshOpts) > 0 {
		newopt = append(newopt, mIncr(defaultopt.RefreshOpts...))
	}
	ress, err := c.MIncrItem(ctx, newopt...)
	if err != nil {
		return nil, err
	}
	return ress, nil
}
func mQueryItemCmds(key string, items ...string) (cmd string, cmdswithoutttl []interface{}) {
	cmd = "TOPK.QUERY"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, item := range items {
		cmdswithoutttl = append(cmdswithoutttl, item)
	}
	return
}

//MQueryItem 查看物品当前是否在topk序列
//@params  items ...string 待查看物品
//@returns map[string]bool 物品:是否在topk序列
func (c *TopK) MQueryItem(ctx context.Context, items ...string) (map[string]bool, error) {
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

//QueryItem 查看物品当前是否在topk序列
//@params  item ...string 待查看物品
//@returns bool 是否在topk序列,true为在
func (c *TopK) QueryItem(ctx context.Context, item string) (bool, error) {
	ress, err := c.MQueryItem(ctx, item)
	if err != nil {
		return false, err
	}
	return ress[item], nil
}

func mCountItemCmds(key string, items ...string) (cmd string, cmdswithoutttl []interface{}) {
	cmd = "TOPK.COUNT"
	cmdswithoutttl = append(cmdswithoutttl, cmd, key)
	for _, item := range items {
		cmdswithoutttl = append(cmdswithoutttl, item)
	}
	return
}

//MCountItem topk中检查item的计数,注意这个计数永远不会高于实际数量并且可能会更低.
//@params items ...string 要检查的物品
//@return map[string]int64 item对应的计数
func (c *TopK) MCountItem(ctx context.Context, items ...string) (map[string]int64, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mCountItemCmds(c.Key(), items...)
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

//CountItem topk中检查item的计数,注意这个计数永远不会高于实际数量并且可能会更低.
//@params item string 要检查的物品
//@return map[string]int64 item对应的计数
func (c *TopK) CountItem(ctx context.Context, item string) (int64, error) {
	ress, err := c.MCountItem(ctx, item)
	if err != nil {
		return 0, err
	}
	return ress[item], nil
}

//TopKInfo TopK状态信息
type TopKInfo struct {
	K     int64   `json:"K"`
	Width int64   `json:"Width"`
	Depth int64   `json:"Depth"`
	Decay float64 `json:"Decay"`
}

//Info 查看指定TopK的状态
func (c *TopK) Info(ctx context.Context) (*TopKInfo, error) {
	cmd := []interface{}{"TOPK.INFO", c.Key()}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if len(infos) != 8 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, errors.New("cannot parser TOPK.INFO result to []interface{}")
	}
	// log.Info("get info", log.Dict{"res": res})
	k, ok := infos[1].(int64)
	if !ok {
		return nil, errors.New("cannot parser K result to int64")
	}
	width, ok := infos[3].(int64)
	if !ok {
		return nil, errors.New("cannot parser Width result to int64")
	}
	depth, ok := infos[5].(int64)
	if !ok {
		return nil, errors.New("cannot parser Depth result to int64")
	}
	decay_s, ok := infos[7].(string)
	if !ok {
		return nil, errors.New("cannot parser Decay result to int64")
	}
	decay, err := strconv.ParseFloat(decay_s, 64)
	if err != nil {
		return nil, err
	}
	info := TopKInfo{
		K:     k,
		Width: width,
		Depth: depth,
		Decay: decay,
	}
	return &info, nil
}

//List 列出全部topk物品
func (c *TopK) List(ctx context.Context) ([]string, error) {
	cmd := []interface{}{"TOPK.LIST", c.Key()}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if !ok {
		return nil, errors.New("cannot parser TOPK.LIST result to []interface{}")
	}
	result := make([]string, len(infos))
	for index, itemi := range infos {
		item, ok := itemi.(string)
		if !ok {
			return nil, errors.New("cannot parser TOPK.LIST result to string")
		}
		result[index] = item
	}
	return result, nil
}

type CountItem struct {
	Item  string `json:"Item"`
	Count int64  `json:"Count"`
}

//ListWithCount 列出全部topk物品及其对应的权重
func (c *TopK) ListWithCount(ctx context.Context) ([]*CountItem, error) {
	cmd := []interface{}{"TOPK.LIST", c.Key(), "WITHCOUNT"}
	res, err := c.Client().Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	infos, ok := res.([]interface{})
	if !ok {
		return nil, errors.New("cannot parser TOPK.LIST result to []interface{}")
	}
	result := []*CountItem{}
	i := 0
	for i < len(infos) {
		item, ok1 := infos[i].(string)
		if !ok1 {
			return nil, errors.New("cannot parser TOPK.LIST result to string")
		}
		count, ok2 := infos[i+1].(int64)
		if !ok2 {
			return nil, errors.New("cannot parser TOPK.LIST result to int64")
		}
		ci := CountItem{
			Item:  item,
			Count: count,
		}
		result = append(result, &ci)
		i += 2
	}
	return result, nil
}
