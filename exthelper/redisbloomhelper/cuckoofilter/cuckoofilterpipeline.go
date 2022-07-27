//cuckoofilter redis扩展[RedisBloom](https://github.com/RedisBloom/RedisBloom)帮助模块,用于处理布隆过滤器
package cuckoofilter

import (
	"context"
	"errors"
	"time"

	// log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//AddItemPipe cuckoofilter中设置物品,
//注意如果不设置NX则会重复向表中插入数据,返回全部为false,尽量带nx插入
//@params key string 使用的key
//@params item string 要插入的物品
//@params opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns bool 设置的物品是否已经存在,true表示已经存在,不会插入
func AddItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string, opts ...optparams.Option[AddOpts]) (*redisbloomhelper.BoolValPlacehold, error) {
	defaultopt := AddOpts{
		NX: true,
	}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	cmd, params, cmdswithoutttl := addItemCmds(key, item, &defaultopt)

	res := redisbloomhelper.BoolValPlacehold{
		Ck:      cmd,
		Reverse: true,
	}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)

	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//MAddItemPipe cuckoofilter中设置多个物品,使用的是INSERT或者INSERTNX命令
//注意如果不设置NX则会重复向表中插入数据,返回全部为false,尽量带nx插入
//@params key string 使用的key
//@params item []string 要插入的物品
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns map[string]bool 设置的物品是否在设置前已经存在,true表示已经存在,并未插入
func MAddItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items []string, opts ...optparams.Option[AddOpts]) (*redisbloomhelper.BoolMapPlacehold, error) {
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
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}

	cmd, params, cmdswithoutttl := mAddItemCmds(key, items, &defaultopt)
	res := redisbloomhelper.BoolMapPlacehold{Items: items, Ck: cmd, Reverse: true}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//ExistsItemPipe cuckoofilter中检查是否已经存在
//@params key string 使用的key
//@params item string 待检查的物品
//@return bool 物品是否已经存在
func ExistsItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) *redisbloomhelper.BoolValPlacehold {
	cmd := "CF.EXISTS"
	res := redisbloomhelper.BoolValPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key, item)
	return &res
}

//MExistsItemPipe cuckoofilter中检查复数物品是否已经存在
//@params key string 使用的key
//@params item ...string 待检查的物品
//@return  map[string]bool 检查的物品是否已经存在
func MExistsItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items ...string) (*redisbloomhelper.BoolMapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	cmd, cmdswithoutttl := mExistsItemCmds(key, items...)
	res := redisbloomhelper.BoolMapPlacehold{Items: items, Ck: cmd}
	res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	return &res, nil
}

type InfoPlacehold struct {
	Ck  string
	Cmd *redis.Cmd
}

func (r *InfoPlacehold) Result() (*CuckoofilterInfo, error) {
	res := r.Cmd.Val()
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

//InfoPipe 查看指定cuckoofilter的状态
//@params key string 使用的key
func InfoPipe(pipe redis.Pipeliner, ctx context.Context, key string) *InfoPlacehold {
	cmd := "CF.INFO"
	res := InfoPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key)
	return &res
}

//ReservePipe 创建一个cuckoofilter,如果有设置maxttl,则会同时为其设置一个过期
//@params key string 使用的key
//@params capacity int64 容量,预估物品的数量,容量越大检索效率越低,但如果超出容量则会默认使用子过滤器扩容,这对检索效率的影响更大
//@params opts ...optparams.Option[ReserveOpts] 可选设置项
func ReservePipe(pipe redis.Pipeliner, ctx context.Context, key string, capacity int64, opts ...optparams.Option[ReserveOpts]) *redis.Cmd {
	defaultopt := ReserveOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl := reserveCmds(key, capacity, &defaultopt)
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

//InsertPipe 向一个cuckoofilter中插入数据,如果不存在则创建
//@params key string 使用的key
//@params items []string 待插入物品
//@params opts ...optparams.Option[InsertOpts] 可选设置项
func InsertPipe(pipe redis.Pipeliner, ctx context.Context, key string, items []string, opts ...optparams.Option[InsertOpts]) (*redisbloomhelper.BoolMapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := InsertOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)

	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	cmd, params, cmdswithoutttl := insertCmds(key, items, &defaultopt)
	res := redisbloomhelper.BoolMapPlacehold{Items: items, Ck: cmd, Reverse: true}

	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//CountItemPipe cuckoofilter中检查item存在的个数
//@params key string 使用的key
//@params item string
//@return int64 计数item的个数,如果报错则返回0
func CountItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) *redisbloomhelper.Int64ValPlacehold {
	cmd := "CF.COUNT"
	res := redisbloomhelper.Int64ValPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key, item)
	return &res
}

//DelItemPipe cuckoofilter中检查item存在的个数
//@params key string 使用的key
//@params item string
//@return bool item是否删除成功,true为删除成功,false表示不存在item无法删除
func DelItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) *redisbloomhelper.BoolValPlacehold {
	cmd := "CF.DEL"
	res := redisbloomhelper.BoolValPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key, item)
	return &res
}
