//Package clientkeybatch  redis的keybatch包装
package clientkeybatch

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/exception"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//ClientKeyBatch 描述任意一种的一批key对象
type ClientKeyBatch struct {
	Keys              []string
	Opt               *clientkey.Option
	autorefreshtaskid cron.EntryID //定时任务id
	Client            redis.UniversalClient
}

//New 创建一个新的key对象
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params opts ...*KeyOption key的选项
func New(client redis.UniversalClient, keys []string, opts ...*clientkey.Option) (*ClientKeyBatch, error) {
	// _, ok := client.(redis.Pipeliner)
	// if ok {
	// 	return nil, ErrClientCannotBePipeliner
	// }
	k := new(ClientKeyBatch)
	k.Client = client
	k.Keys = keys
	if len(keys) == 0 {
		return nil, ErrKeysMustMoreThanOne
	}
	switch len(opts) {
	case 0:
		{
			k.Opt = &clientkey.Option{}
			return k, nil
		}
	case 1:
		{
			opt := opts[0]
			if opt == nil {
				k.Opt = &clientkey.Option{}
				return k, nil
			}
			if opt.TaskCron == nil && opt.AutoRefreshInterval != "" {
				opt.TaskCron = cron.New()
				opt.TaskCron.Start()
			}
			k.Opt = opt
			return k, nil
		}
	default:
		{
			return nil, exception.ErrParamOptsLengthMustLessThan2
		}
	}
}

//NewFromClientKey 从多个clientkey转变为一个keybatch
func NewFromClientKey(ctx context.Context, opt *clientkey.Option, checkType bool, keys ...*clientkey.ClientKey) (*ClientKeyBatch, error) {
	keysLen := len(keys)
	if keysLen == 0 {
		return nil, ErrKeysMustMoreThanOne
	}
	keyclient := keys[0].Client
	keytype, err := keys[0].Type(ctx)
	if err != nil {
		return nil, err
	}
	if keysLen == 1 {
		return New(keyclient, []string{keys[0].Key}, opt)
	}
	keystrings := []string{}
	for _, key := range keys[1:] {
		if key.Client != keyclient {
			return nil, ErrKeysMustSameClient
		}
		if checkType {
			_keytype, err := key.Type(ctx)
			if err != nil {
				return nil, err
			}
			if _keytype != keytype {
				return nil, ErrKeysMustSameType
			}
			keystrings = append(keystrings, key.Key)
		}
	}
	return New(keyclient, keystrings, opt)
}

//AllExists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKeyBatch) AllExists(ctx context.Context) (bool, error) {
	res, err := k.Client.Exists(ctx, k.Keys...).Result()
	if err != nil {
		return false, err
	}
	if res == int64(len(k.Keys)) {
		return true, nil
	}
	return false, nil
}

//AnyExists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKeyBatch) AnyExists(ctx context.Context) (bool, error) {
	res, err := k.Client.Exists(ctx, k.Keys...).Result()
	if err != nil {
		return false, err
	}
	if res > 0 {
		return true, nil
	}
	return false, nil
}

//Types 查看这批key的的类型
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@returns map[string]string  key为键，value为键的类型，如果键不存在，则为空字符串
//@returns error 执行时的错误
func (k *ClientKeyBatch) Types(ctx context.Context) (map[string]string, error) {
	futs := map[string]*redis.StatusCmd{}
	pipe := k.Client.TxPipeline()
	for _, key := range k.Keys {
		fut := pipe.Type(ctx, key)
		futs[key] = fut
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	res := map[string]string{}
	for key, cmder := range futs {
		v := cmder.Val()
		if v == "none" {
			res[key] = ""
		} else {
			res[key] = v
		}
	}
	return res, nil
}

//HasSameType 判断这一批键是否都是同一类型
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@returns bool 是否类型一致
//@returns string 当类型一致时返回类型名
//@returns error 执行时的错误
func (k *ClientKeyBatch) HasSameType(ctx context.Context) (bool, string, error) {

	kt, err := k.Types(ctx)
	if err != nil {
		return false, "", err
	}
	a := kt[k.Keys[0]]
	for _, t := range kt {
		if t != a {
			return false, "", nil
		}
	}
	return true, a, nil
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@returns map[string]time.Duration  key为键，value为键的类型，如果键不存在，则为-2，如果键未设置过期则返回-1
//@returns error 执行时的错误
func (k *ClientKeyBatch) TTL(ctx context.Context) (map[string]time.Duration, error) {
	futs := map[string]*redis.DurationCmd{}
	pipe := k.Client.TxPipeline()
	for _, key := range k.Keys {
		fut := pipe.TTL(ctx, key)
		futs[key] = fut
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	res := map[string]time.Duration{}
	for key, cmder := range futs {
		v := cmder.Val()
		res[key] = v
	}
	return res, nil
}

//写操作

//Delete 删除key
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKeyBatch) Delete(ctx context.Context) error {

	k.Client.Del(ctx, k.Keys...).Result()
	_, err := k.Client.Del(ctx, k.Keys...).Result()
	if err != nil {
		return err
	}
	return nil
}

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKeyBatch) RefreshTTL(ctx context.Context) error {
	if k.Opt.MaxTTL != 0 {
		pipe := k.Client.TxPipeline()
		for _, key := range k.Keys {
			pipe.Expire(ctx, key, k.Opt.MaxTTL)
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}
		return nil
	}
	return ErrBatchNotSetMaxTLL
}

//AutoRefresh 自动刷新key的过期时间
func (k *ClientKeyBatch) AutoRefresh() error {
	if k.autorefreshtaskid != 0 {
		return ErrAutoRefreshTaskHasBeenSet
	}
	if k.Opt.AutoRefreshInterval == "" {
		return ErrAutoRefreshTaskInterval
	}
	taskid, err := k.Opt.TaskCron.AddFunc(k.Opt.AutoRefreshInterval, func() {
		ctx := context.Background()
		err := k.RefreshTTL(ctx)
		if err != nil {
			log.Error("自动刷新key的过期时间失败", log.Dict{"err": err.Error(), "keys": k.Keys})
		}
	})
	if err != nil {
		return err
	}
	k.autorefreshtaskid = taskid
	return nil
}

//StopAutoRefresh 取消自动更新缓存
//@params force bool 强制停下整个定时任务cron对象
func (k *ClientKeyBatch) StopAutoRefresh(force bool) error {
	if force == true {
		if k.Opt.AutoRefreshInterval == "" {
			return ErrAutoRefreshTaskHNotSetYet
		}
		if k.autorefreshtaskid != 0 {
			k.Opt.TaskCron.Remove(k.autorefreshtaskid)
			k.autorefreshtaskid = 0
		}
		k.Opt.TaskCron.Stop()
		return nil
	}
	if k.Opt.AutoRefreshInterval == "" || k.autorefreshtaskid == 0 {
		return ErrAutoRefreshTaskHNotSetYet
	}
	k.Opt.TaskCron.Remove(k.autorefreshtaskid)
	k.autorefreshtaskid = 0
	return nil
}

//ToArray 将batch转化为key的序列
func (k *ClientKeyBatch) ToArray() ([]*clientkey.ClientKey, error) {
	res := []*clientkey.ClientKey{}
	for _, keystring := range k.Keys {
		key, err := clientkey.New(k.Client, keystring, k.Opt)
		if err != nil {
			return nil, err
		}
		res = append(res, key)
	}
	return res, nil
}
