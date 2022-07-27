package topk

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//InitPipe 初始化TopK
//@params key string 使用的key
//@params topk int64 结构保存的topk
//@params opts ...optparams.Option[InitOpts] 可选参数
func InitPipe(pipe redis.Pipeliner, ctx context.Context, key string, topk int64, opts ...optparams.Option[InitOpts]) *redis.Cmd {
	defaultopt := InitOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl := initCmds(key, topk, &defaultopt)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	if ttl > 0 {
		return middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)

	} else {
		return pipe.Do(ctx, cmdswithoutttl...)
	}
}

//MIncrItemPipe 为物品添加计数
//@params key string 使用的key
//@params  opts ...optparams.Option[IncrOpts] 请求设置项,请使用`IncrWithItems`或`IncrWithItemMap`或`IncrWithIncrItems`设置物品及其增量
//@returns *redisbloomhelper.StringListValPlacehold 新增后被挤出队列的物品的占位符,使用`.Result()`获取值
func MIncrItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, opts ...optparams.Option[IncrOpts]) (*redisbloomhelper.StringListValPlacehold, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl, err := mIncrItemCmds(key, &defaultopt)
	if err != nil {
		return nil, err
	}
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	res := redisbloomhelper.StringListValPlacehold{
		Ck: cmd,
	}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)

	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//IncrItemPipe 为物品添加计数
//@params key string 使用的key
//@params  item string 物品及其对应的增量
//@returns *redisbloomhelper.StringListValPlacehold 新增后被挤出队列的物品的占位符,使用`.Result()`获取值
func IncrItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string, opts ...optparams.Option[IncrOpts]) (*redisbloomhelper.StringListValPlacehold, error) {
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
	ress, err := MIncrItemPipe(pipe, ctx, key, newopt...)
	if err != nil {
		return nil, err
	}
	return ress, nil
}

//MQueryItemPipe 查看物品当前是否在topk序列
//@params key string 使用的key
//@params  items ...string 待查看物品
//@returns *redisbloomhelper.BoolMapPlacehold 物品:是否在topk序列的占位符,使用`.Result()`获取值
func MQueryItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items ...string) (*redisbloomhelper.BoolMapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mQueryItemCmds(key, items...)
	res := redisbloomhelper.BoolMapPlacehold{
		Ck:    cmd,
		Items: items,
	}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res, nil
}

//QueryItemPipe 查看物品当前是否在topk序列
//@params key string 使用的key
//@params  item ...string 待查看物品
//@returns *redisbloomhelper.OneBoolMapPlacehold 是否在topk序列(true为在)的占位符,使用`.Result()`获取值
func QueryItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) (*redisbloomhelper.OneBoolMapPlacehold, error) {
	ress, err := MQueryItemPipe(pipe, ctx, key, item)
	if err != nil {
		return nil, err
	}
	res := redisbloomhelper.OneBoolMapPlacehold{
		MapPlacehold: ress,
		Item:         item,
	}
	return &res, nil
}

//MCountItemPipe topk中检查item的计数,注意这个计数永远不会高于实际数量并且可能会更低.
//@params key string 使用的key
//@params items ...string 要检查的物品
//@return *redisbloomhelper.Int64MapPlacehold item对应的计数的占位符,使用`.Result()`获取值
func MCountItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items ...string) (*redisbloomhelper.Int64MapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mCountItemCmds(key, items...)
	res := redisbloomhelper.Int64MapPlacehold{
		Ck:    cmd,
		Items: items,
	}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res, nil
}

//CountItemPipe topk中检查item的计数,注意这个计数永远不会高于实际数量并且可能会更低.
//@params key string 使用的key
//@params item string 要检查的物品
//@return *redisbloomhelper.OneInt64MapPlacehold item对应的计数的占位符,使用`.Result()`获取值
func CountItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) (*redisbloomhelper.OneInt64MapPlacehold, error) {
	ress, err := MCountItemPipe(pipe, ctx, key, item)
	if err != nil {
		return nil, err
	}
	res := redisbloomhelper.OneInt64MapPlacehold{
		MapPlacehold: ress,
		Item:         item,
	}
	return &res, nil
}

type InfoPlacehold struct {
	Ck  string
	Cmd *redis.Cmd
}

func (r *InfoPlacehold) Result() (*TopKInfo, error) {
	res := r.Cmd.Val()
	infos, ok := res.([]interface{})
	if len(infos) != 8 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, errors.New("cannot parser TOPK.INFO result to []interface{}")
	}
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

//InfoPipe 查看指定TopK的状态
//@params key string 使用的key
func InfoPipe(pipe redis.Pipeliner, ctx context.Context, key string) *InfoPlacehold {
	cmd := "TOPK.INFO"
	cmdswithoutttl := []interface{}{cmd, key}
	res := InfoPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res
}

//ListPipe 列出全部topk物品
//@params key string 使用的key
func ListPipe(pipe redis.Pipeliner, ctx context.Context, key string) *redisbloomhelper.StringListValPlacehold {
	cmd := "TOPK.LIST"
	cmdswithoutttl := []interface{}{cmd, key}
	res := redisbloomhelper.StringListValPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res
}

type CountItemPlacehold struct {
	Ck  string
	Cmd *redis.Cmd
}

func (r *CountItemPlacehold) Result() ([]*CountItem, error) {
	res := r.Cmd.Val()
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

//ListWithCountPipe 列出全部topk物品及其对应的权重
//@params key string 使用的key
func ListWithCountPipe(pipe redis.Pipeliner, ctx context.Context, key string) *CountItemPlacehold {
	cmd := "TOPK.LIST"
	cmdswithoutttl := []interface{}{cmd, key, "WITHCOUNT"}
	res := CountItemPlacehold{}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res
}
