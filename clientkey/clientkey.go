//Package key redis的key包装
package clientkey

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//ClientKey 描述任意一种的单个key对象
type ClientKey struct {
	Key               string
	Opt               Options
	autorefreshtaskid cron.EntryID //定时任务id
	Client            redis.UniversalClient
}

//Option 设置key行为的选项
//@attribute MaxTTL time.Duration 为0则不设置过期
//@attribute AutoRefresh string 需要为crontab格式的字符串,否则不会自动定时刷新
type Options struct {
	MaxTTL              time.Duration
	AutoRefreshInterval string
	TaskCron            *cron.Cron
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

// func (emptyOption) apply(*Options) {}
type funcOption struct {
	f func(*Options)
}

func (fo *funcOption) Apply(do *Options) {
	fo.f(do)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

//WithMaxTTL 设置最大过期时间
func WithMaxTTL(maxttl time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.MaxTTL = maxttl
	})
}

//WithAutoRefreshInterval 设置自动刷新过期时间的设置
func WithAutoRefreshInterval(autoRefreshInterval string) Option {
	return newFuncOption(func(o *Options) {
		o.AutoRefreshInterval = autoRefreshInterval
	})
}

//WithTaskCron 设置定时器
func WithTaskCron(taskCron *cron.Cron) Option {
	return newFuncOption(func(o *Options) {
		o.TaskCron = taskCron
	})
}

//New 创建一个新的key对象
//@params client redis.UniversalClient 客户端对象
//@params key string bitmap使用的key
//@params opts ...*KeyOption key的选项
func New(client redis.UniversalClient, key string, opts ...Option) *ClientKey {
	k := new(ClientKey)
	k.Client = client
	k.Key = key
	defaultopt := Options{}
	k.Opt = defaultopt
	for _, opt := range opts {
		opt.Apply(&k.Opt)
	}
	if k.Opt.TaskCron == nil && k.Opt.AutoRefreshInterval != "" {
		k.Opt.TaskCron = cron.New()
		k.Opt.TaskCron.Start()
	}
	return k
}

//Exists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKey) Exists(ctx context.Context) (bool, error) {
	res, err := k.Client.Exists(ctx, k.Key).Result()
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
func (k *ClientKey) Type(ctx context.Context) (string, error) {
	typeName, err := k.Client.Type(ctx, k.Key).Result()
	if err != nil {
		return "", err
	}
	if typeName == "none" {
		return "", ErrKeyNotExist
	}
	return typeName, nil
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKey) TTL(ctx context.Context) (time.Duration, error) {
	res, err := k.Client.TTL(ctx, k.Key).Result()
	if err != nil {
		return 0, err
	}
	if int64(res) == -1 {
		return 0, ErrKeyNotSetExpire
	}
	if int64(res) == -2 {
		return 0, ErrKeyNotExist
	}
	return res, nil
}

//写操作

//Delete 删除key
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKey) Delete(ctx context.Context) error {
	r, err := k.Client.Del(ctx, k.Key).Result()
	if err != nil {
		return err
	}
	if r == 0 {
		return ErrKeyNotExist
	}
	return nil
}

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (k *ClientKey) RefreshTTL(ctx context.Context) error {
	if k.Opt.MaxTTL != 0 {
		res, err := k.Client.Expire(ctx, k.Key, k.Opt.MaxTTL).Result()
		if err != nil {
			return err
		}
		if res == false {
			return ErrKeyNotExist
		}
		return nil
	}
	return ErrKeyNotSetMaxTLL
}

//AutoRefresh 自动刷新key的过期时间
func (k *ClientKey) AutoRefresh() error {
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
			log.Error("自动刷新key的过期时间失败", log.Dict{"err": err.Error(), "key": k.Key})
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
func (k *ClientKey) StopAutoRefresh(force bool) error {
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
