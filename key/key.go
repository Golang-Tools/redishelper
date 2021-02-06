//Package key redis的key包装
package key

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/utils"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//Key 描述任意一种的单个key对象
type Key struct {
	Key               string
	Opt               *Option
	autorefreshtaskid cron.EntryID //定时任务id
	redis.UniversalClient
}

//Option 设置key行为的选项
//@attribute MaxTTL time.Duration 为0则不设置过期
//@attribute AutoRefresh string 需要为crontab格式的字符串,否则不会自动定时刷新
type Option struct {
	MaxTTL              time.Duration
	AutoRefreshInterval string
	TaskCron            *cron.Cron
}

//New 创建一个新的key对象
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params opts ...*KeyOption key的选项
func New(client redis.UniversalClient, key string, opts ...*Option) (*Key, error) {
	k := new(Key)
	k.UniversalClient = client
	k.Key = key
	switch len(opts) {
	case 0:
		{
			k.Opt = &Option{}
			return k, nil
		}
	case 1:
		{
			opt := opts[0]
			if opt == nil {
				k.Opt = &Option{}
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
			return nil, utils.ErrParamOptsLengthMustLessThan2
		}
	}
}

//Exists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Exists(ctx context.Context) (bool, error) {
	res, err := k.UniversalClient.Exists(ctx, k.Key).Result()
	if err != nil {
		return false, err
	}
	if res == 0 {
		return false, nil
	}
	return true, nil
}

//Type 查看key的类型
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Type(ctx context.Context) (string, error) {
	typeName, err := k.UniversalClient.Type(ctx, k.Key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", ErrKeyNotExist
		}
		return "", err
	}
	return typeName, nil
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) TTL(ctx context.Context) (time.Duration, error) {
	res, err := k.UniversalClient.TTL(ctx, k.Key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrKeyNotExist
		}
		return 0, err
	}
	return res, nil
}

//写操作

//Delete 删除key
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) Delete(ctx context.Context) (bool, error) {
	_, err := k.UniversalClient.Del(ctx, k.Key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, ErrKeyNotExist
		}
		return false, err
	}
	return true, nil
}

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *Key) RefreshTTL(ctx context.Context) error {
	ok, err := k.Exists(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrKeyNotExist
	}
	if k.Opt.MaxTTL != 0 {
		_, err := k.UniversalClient.Expire(ctx, k.Key, k.Opt.MaxTTL).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrKeyNotSetMaxTLL
}

//AutoRefresh 自动刷新key的过期时间
func (k *Key) AutoRefresh() error {
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
			log.Error("自动刷新key的过期时间失败", log.Dict{"err": err.Error()})
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
func (k *Key) StopAutoRefresh(force bool) error {
	if force == true {
		if k.Opt.AutoRefreshInterval == "" || k.autorefreshtaskid == 0 {
			return ErrAutoRefreshTaskHNotSetYet
		}
		k.Opt.TaskCron.Remove(k.autorefreshtaskid)
		k.autorefreshtaskid = 0
		return nil
	}
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
