//middlewarehelper 用于规范中间件的模块
//中间件指的是一些有特殊业务用途的模块,
//这个子模块是整个redishelper的一个基本组件
package middlewarehelper

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/namespace"
	"github.com/Golang-Tools/optparams"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

type MiddleWareAbc struct {
	cli               redis.UniversalClient
	key               string
	middlewareType    string
	opt               Options
	autorefreshtaskid cron.EntryID
	logger            *log.Log
}

func New(cli redis.UniversalClient, middlewareType string, opts ...optparams.Option[Options]) (*MiddleWareAbc, error) {
	l := new(MiddleWareAbc)
	l.cli = cli
	l.middlewareType = middlewareType

	l.opt = DefaultOptions
	l.opt.Namespace = append(l.opt.Namespace, middlewareType)
	optparams.GetOption(&l.opt, opts...)
	if l.opt.Key != "" {
		if l.opt.OnlyKey {
			l.key = l.opt.Key
		} else {
			l.key = l.opt.Namespace.FullName(l.opt.Key, namespace.WithRedisStyle())
		}
	} else {
		s, err := l.opt.Namespace.RandomKey(namespace.WithRedisStyle())
		if err != nil {
			return nil, err
		}
		l.key = s
	}
	log.Set(log.WithExtFields(log.Dict{"module": "redishelper", "middleware_type": middlewareType, "key": l.key}))
	l.logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
	return l, nil
}

//Client 获取redis客户端
func (l *MiddleWareAbc) Client() redis.UniversalClient {
	return l.cli
}

//Key 查看组件使用的键
func (l *MiddleWareAbc) Key() string {
	return l.key
}

//MiddlewareType 查看中间件类型
func (l *MiddleWareAbc) MiddlewareType() string {
	return l.middlewareType
}

//MaxTTL 查看组件设置的最大过期时间
func (l *MiddleWareAbc) MaxTTL() time.Duration {
	return l.opt.MaxTTL
}

func (l *MiddleWareAbc) Logger() *log.Log {
	return l.logger
}

//Exists 查看key是否存在
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *MiddleWareAbc) Exists(ctx context.Context) (bool, error) {
	res, err := l.cli.Exists(ctx, l.key).Result()
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
func (l *MiddleWareAbc) Type(ctx context.Context) (string, error) {
	typeName, err := l.cli.Type(ctx, l.key).Result()
	if err != nil {
		return "", err
	}
	if typeName == "none" {
		return "", ErrKeyNotExists
	}
	return typeName, nil
}

//TTL 查看key的剩余时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *MiddleWareAbc) TTL(ctx context.Context) (time.Duration, error) {
	res, err := l.cli.TTL(ctx, l.key).Result()
	if err != nil {
		return 0, err
	}
	if int64(res) == -1 {
		return 0, ErrKeyNotSetExpire
	}
	if int64(res) == -2 {
		return 0, ErrKeyNotExists
	}
	return res, nil
}

//写操作

//Delete 删除key
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *MiddleWareAbc) Delete(ctx context.Context) error {
	r, err := l.cli.Del(ctx, l.key).Result()
	if err != nil {
		return err
	}
	if r == 0 {
		return ErrKeyNotExists
	}
	return nil
}

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (l *MiddleWareAbc) RefreshTTL(ctx context.Context) error {
	if l.opt.MaxTTL != 0 {
		res, err := l.cli.Expire(ctx, l.key, l.opt.MaxTTL).Result()
		if err != nil {
			return err
		}
		if !res {
			return ErrKeyNotExists
		}
		return nil
	}
	return ErrKeyNotSetMaxTLL
}

//AutoRefresh 自动刷新key的过期时间
func (l *MiddleWareAbc) AutoRefresh() error {
	if l.autorefreshtaskid != 0 {
		return ErrAutoRefreshTaskHasBeenSet
	}
	if l.opt.AutoRefreshInterval == "" {
		return ErrAutoRefreshTaskInterval
	}
	taskid, err := l.opt.TaskCron.AddFunc(l.opt.AutoRefreshInterval, func() {
		ctx := context.Background()
		err := l.RefreshTTL(ctx)
		if err != nil {
			l.logger.Error("自动刷新key的过期时间失败", log.Dict{"err": err.Error(), "key": l.key})
		}
	})
	if err != nil {
		return err
	}
	l.autorefreshtaskid = taskid
	return nil
}

type ForceOpt struct {
	Force bool
}

//Force 设置force
func Force() optparams.Option[ForceOpt] {
	return optparams.NewFuncOption(func(o *ForceOpt) {
		o.Force = true
	})
}

//StopAutoRefresh 取消自动更新缓存
//@params force bool 强制停下整个定时任务cron对象
func (l *MiddleWareAbc) StopAutoRefresh(opts ...optparams.Option[ForceOpt]) error {
	opt := ForceOpt{}
	optparams.GetOption(&opt, opts...)
	if opt.Force {
		if l.opt.AutoRefreshInterval == "" {
			return ErrAutoRefreshTaskNotSetYet
		}
		if l.autorefreshtaskid != 0 {
			l.opt.TaskCron.Remove(l.autorefreshtaskid)
			l.autorefreshtaskid = 0
		}
		l.opt.TaskCron.Stop()
		return nil
	}
	if l.opt.AutoRefreshInterval == "" || l.autorefreshtaskid == 0 {
		return ErrAutoRefreshTaskNotSetYet
	}
	l.opt.TaskCron.Remove(l.autorefreshtaskid)
	l.autorefreshtaskid = 0
	return nil
}
