//redisbloomhelper redis扩展[RedisBloom](https://github.com/RedisBloom/RedisBloom)帮助模块,用于处理布隆过滤器
package bloomfilterhelper

import (
	"context"
	"errors"
	"fmt"
	"time"

	// log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
)

//AddItemPipe 在pipeline中设置布隆过器中的物品
//@params key string 使用的key
//@params item string 被设置的物品
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns *BoolValPlacehold 结果的占位符
func AddItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string, opts ...optparams.Option[AddOpts]) (*redisbloomhelper.BoolValPlacehold, error) {
	defaultopt := AddOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	cmd := "BF.ADD"
	res := redisbloomhelper.BoolValPlacehold{
		Ck:      cmd,
		Reverse: true,
	}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, item)

	} else {
		res.Cmd = pipe.Do(ctx, cmd, key, item)
	}
	return &res, nil
}

//MAddItemPipe 在pipeline中设置布隆过器的多个物品
//@params key string 使用的key
//@params items []string
//@params  opts ...optparams.Option[AddOpts] 设置add行为的附加参数
//@returns map[string]bool 设置的物品是否在设置前已经存在
func MAddItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, items []string, opts ...optparams.Option[AddOpts]) (*redisbloomhelper.BoolMapPlacehold, error) {
	itemlen := len(items)
	if itemlen == 0 {
		return nil, redisbloomhelper.ErrItemListEmpty
	}
	defaultopt := AddOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	var ttl time.Duration = 0
	if refreshopt.TTL > 0 && refreshopt.RefreshTTL == middlewarehelper.RefreshTTLType_Refresh {
		ttl = refreshopt.TTL
	}
	cmd, params, cmdswithoutttl := mAddItemCmds(key, items)
	res := redisbloomhelper.BoolMapPlacehold{Items: items, Ck: cmd, Reverse: true}
	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}

//ExistsItemPipe 在pipeline中设置布隆过器中检查是否已经存在
//@params key string 使用的key
//@params item string
//@return bool 物品是否已经存在
func ExistsItemPipe(pipe redis.Pipeliner, ctx context.Context, key string, item string) *redisbloomhelper.BoolValPlacehold {
	cmd := "BF.EXISTS"
	res := redisbloomhelper.BoolValPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key, item)
	return &res
}

//MExistsItemPipe Pipeline中布隆过器中检查复数物品是否已经存在
//@params key string 使用的key
//@params item string
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

func (r *InfoPlacehold) Result() (*BloomFilterInfo, error) {
	res := r.Cmd.Val()
	infos, ok := res.([]interface{})
	if len(infos) != 10 {
		return nil, redisbloomhelper.ErrResultLenNotMatch
	}
	if !ok {
		return nil, fmt.Errorf("cannot parser %s result to []interface{}", r.Ck)
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

//InfoPipe Pipeline中查看指定bloomfilter的状态
//@params key string 使用的key
func InfoPipe(pipe redis.Pipeliner, ctx context.Context, key string) *InfoPlacehold {
	cmd := "BF.INFO"
	res := InfoPlacehold{
		Ck: cmd,
	}
	res.Cmd = pipe.Do(ctx, cmd, key)
	return &res
}

//ReservePipe Pipeline中创建一个布隆过滤器,如果有设置maxttl,则会同时为其设置一个过期
//@params key string 使用的key
//@params capacity int64 容量,预估物品的数量,容量越大检索效率越低,但如果超出容量则会默认使用子过滤器扩容,这对检索效率的影响更大
//@params error_rate float64 碰撞率,碰撞率设置的越低使用的hash函数越多,使用的空间也越大,检索效率也越低
//@params opts ...optparams.Option[ReserveOpts] 可选设置项
func ReservePipe(pipe redis.Pipeliner, ctx context.Context, key string, capacity int64, error_rate float64, opts ...optparams.Option[ReserveOpts]) *redis.Cmd {
	defaultopt := ReserveOpts{}
	optparams.GetOption(&defaultopt, opts...)
	refreshopt := middlewarehelper.RefreshOpt{}
	optparams.GetOption(&refreshopt, defaultopt.RefreshOpts...)
	cmd, params, cmdswithoutttl := mReserveCmds(key, capacity, error_rate, &defaultopt)
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

//InsertPipe Pipeline中向布隆过滤器中插入数据,如果不存在就创建
//@params key string 使用的key
//@params items []string 待插入数据
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
	cmd, params, cmdswithoutttl := mInsertCmds(key, items, &defaultopt)

	res := redisbloomhelper.BoolMapPlacehold{Items: items, Ck: cmd, Reverse: true}

	if ttl > 0 {
		res.Cmd = middlewarehelper.DoCmdWithTTL(pipe, ctx, cmd, key, ttl, params...)
	} else {
		res.Cmd = pipe.Do(ctx, cmdswithoutttl...)
	}
	return &res, nil
}
