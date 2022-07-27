package countminsketch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//InitPipe 初始化CountMinSketch
//@params key string 目标key
//@params opts ...optparams.Option[InitOpts] 注意InitWithProbability和InitWithDIM必选一个,如果选了超过一个则会默认取第一个
func InitPipe(pipe redis.Pipeliner, ctx context.Context, key string, opts ...optparams.Option[InitOpts]) (*redis.Cmd, error) {
	if len(opts) < 1 {
		return nil, redisbloomhelper.ErrMustChooseOneParam
	}
	defaultopt := InitOpts{}
	optparams.GetOption(&defaultopt, opts[0])
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl, err := initCmds(key, &defaultopt)
	if err != nil {
		return nil, err
	}
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	if ttl > 0 {
		cmd := middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
		return cmd, nil

	} else {
		cmd := pipe.Do(ctx, cmdswithoutttl...)
		return cmd, nil
	}
}

//MIncrItemPipe 为物品添加计数
//@params key string 目标key
//@params  opts ...optparams.Option[IncrOpts] 增加数据时的设置,请使用`IncrWithItemMap`或`IncrWithIncrItems`设置物品及其增量
//@returns *redisbloomhelper.Int64MapPlacehold 物品在新增后的存量
func MIncrItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, opts ...optparams.Option[IncrOpts]) (*redisbloomhelper.Int64MapPlacehold, error) {
	defaultopt := IncrOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)

	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	itemlen := len(defaultopt.MincrItems)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, params, cmdswithoutttl, items := mIncrItemCmds(key, &defaultopt)

	res := redisbloomhelper.Int64MapPlacehold{Items: items, Ck: cmd}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//IncrItemPipe 为物品添加计数
//@params key string 目标key
//@params item string 要增加的物品
//@params opts ...optparams.Option[IncrOpts] 增加数据时的设置,可以使用`IncrWithIncrement`额外指定物品的增量
//@returns *redisbloomhelper.OneInt64MapPlacehold 物品在新增后的存量
func IncrItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string, opts ...optparams.Option[IncrOpts]) (*redisbloomhelper.OneInt64MapPlacehold, error) {
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
	ress, err := MIncrItemPipe(pipe, ctx, key, newopt...)
	if err != nil {
		return nil, err
	}
	res := redisbloomhelper.OneInt64MapPlacehold{
		MapPlacehold: ress,
		Item:         item,
	}
	return &res, nil
}

//MQueryItemPipe 查看物品当前计数
//@params key string 目标key
//@params items ...string 待查看物品
//@returns *redisbloomhelper.Int64MapPlacehold 物品在新增后的存量
func MQueryItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items ...string) (*redisbloomhelper.Int64MapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mQueryItemCmds(key, items...)
	res := &redisbloomhelper.Int64MapPlacehold{Ck: cmd, Items: items}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return res, nil
}

//QeryItemPipe 查看物品当前计数
//@params key string 目标key
//@params item string 代查看物品
//@returns *redisbloomhelper.OneInt64MapPlacehold 物品在新增后的存量
func QueryItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) (*redisbloomhelper.OneInt64MapPlacehold, error) {
	ress, err := MQueryItemPipe(pipe, ctx, key, item)
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

func (r *InfoPlacehold) Result() (*CountMinSketchInfo, error) {
	res := r.Cmd.Val()
	infos, ok := res.([]interface{})
	if len(infos) != 6 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, fmt.Errorf("cannot parser %s result to []interface{}", r.Ck)
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

//InfoPipe 查看指定CountMinSketch的状态
//@params key string 目标key
func InfoPipe(pipe redis.Pipeliner, ctx context.Context, key string) *InfoPlacehold {
	cmd := "CMS.INFO"
	cmdswithoutttl := []interface{}{cmd, key}
	res := InfoPlacehold{Ck: cmd}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res
}

//MergePipe pipeline中将几个CountMinSketch合并到指定的key
//@params key string 合并到的目标key
//@params sources []*WeightedCountMinSketchKey 用于merge的其他CountMinSketch和其设置的权重序列
//@params opts ...optparams.Option[InitOpts] 创建新CountMinSketch的设置,如果有设置则会先创建key再进行merge,否则认为key已经存在
//@returns *CountMinSketch 融合了自己和othersources信息的新CountMinSketch对象
func MergePipe(pipe redis.Pipeliner, ctx context.Context, key string, sources []*WeightedCountMinSketchKey, opts ...optparams.Option[InitOpts]) (*redis.Cmd, error) {
	sourcelen := len(sources)
	if sourcelen < 2 {
		return nil, redisbloomhelper.ErrNeedMoreThan2Source
	}
	if len(opts) > 0 {
		InitPipe(pipe, ctx, key, opts...)
	}
	_, _, cmdswithoutttl := mergeCmds(key, sources...)
	cmd := pipe.Do(ctx, cmdswithoutttl...)
	return cmd, nil
}
