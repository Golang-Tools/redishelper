//Package cache 缓存对象
//更新缓存往往是竞争更新,因此需要使用分布式锁避免重复计算,同时等待更新完成后再取数据.
package cache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"

	"github.com/Golang-Tools/redishelper/v2/lock"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//Cachefunc 缓存函数结果
type Cachefunc func() ([]byte, error)

//Cache 缓存
//缓存对象的锁可以和缓存不使用同一个redis客户端,甚至可以不用redis,只要他满足Canlock接口
//缓存设置UpdatePeriod后会自动定时同步数据
type Cache struct {
	*middlewarehelper.MiddleWareAbc
	opt        Options
	latestHash string     //上次跟新后保存数据的hash,用于避免重复更新
	updateFunc Cachefunc  //更新缓存的函数
	c          *cron.Cron //定时任务对象
}

//New 创建一个缓存实例
//updatePeriod会取最后设置的非空字符串
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Cache, error) {
	cache := new(Cache)
	cache.opt = Defaultopt
	optparams.GetOption(&cache.opt, opts...)
	m, err := middlewarehelper.New(cli, "cache", cache.opt.MiddlewareOpts...)
	if err != nil {
		return nil, err
	}
	cache.MiddleWareAbc = m
	return cache, nil
}

//RegistUpdateFunc 注册缓存函数到对象
func (c *Cache) RegistUpdateFunc(fn Cachefunc) error {
	if c.updateFunc != nil {
		return ErrUpdateFuncAlreadyRegisted
	}
	c.updateFunc = fn
	return nil
}

// 写操作

//Update 将结果更新到缓存
// 每次更新会判断是否有变化,没有变化如果key设置有MAXTTL则只刷新过期时间,有变化则会更新变化并刷新过期时间
// 如果设置有锁则使用锁限制注册函数执行
//@param force ForceLevelType 强制模式
func (c *Cache) update(force ForceLevelType) ([]byte, error) {
	var res []byte
	var reserr error
	if force == ForceLevel__NOCONSTRAINT {
		res, reserr = c.updateFunc()
	} else {
		//限制器限制执行防止被击穿
		if c.opt.Limiter != nil {
			ctx, cancel := context.WithTimeout(context.Background(), c.opt.QueryAutoUpdateCacheTimeout)
			defer cancel()
			canflood, err := c.opt.Limiter.Flood(ctx, 1)
			if err != nil {
				if force == ForceLevel__STRICT {
					return nil, err
				}
				c.Logger().Error("cache's limiter get error", map[string]any{"err": err.Error()})
			}
			if !canflood {
				return nil, ErrLimiterNotAllow
			}
			res, reserr = c.updateFunc()
		} else {
			res, reserr = c.updateFunc()
		}
	}
	return res, reserr
}

//saveToCache 将数据存至缓存并刷新过期时间
func (c *Cache) saveToCache(ctx context.Context, res []byte) {
	h := md5.New()
	h.Write(res)
	resMd5 := hex.EncodeToString(h.Sum(nil))
	//结果写入缓存
	if c.latestHash == "" || c.latestHash != resMd5 {
		_, err := c.Client().Set(ctx, c.Key(), res, c.MaxTTL()).Result()
		if err != nil {
			c.Logger().Debug("set cache get error", map[string]any{"err": err.Error()})
		} else {
			c.Logger().Debug("set cache succeed")
			c.latestHash = resMd5
		}
	} else {
		//结果未更新则根据设置判断是否需要续期
		if c.opt.AlwaysRefreshTTL && c.MaxTTL() > 0 {
			err := c.RefreshTTL(ctx)
			if err != nil {
				c.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
			}
		}
	}
}

//ActiveUpdate 主动更新函数,可以被用作定时主动更新,也可以通过监听外部信号来实现更新
func (c *Cache) ActiveUpdate() {
	if c.opt.Lock != nil {
		ctx, cancel := context.WithTimeout(context.Background(), c.opt.QueryLockTimeout)
		defer cancel()
		err := c.opt.Lock.Lock(ctx)
		if err != nil {
			if err == lock.ErrAlreadyLocked {
				c.Logger().Info("locked,not do process")
				return
			} else {
				c.Logger().Error("lock error", map[string]any{"err": err.Error()})
			}
		}
		defer c.opt.Lock.Unlock(ctx)
	}
	res, err := c.update(ForceLevel__STRICT)
	if err != nil {
		c.Logger().Error("do update process get error", map[string]any{"err": err.Error()})
		return
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), c.opt.QueryAutoUpdateCacheTimeout)
	defer cancel1()
	if len(res) > 0 {
		c.saveToCache(ctx1, res)
	} else {
		//获取的数据为空
		switch c.opt.EmptyResCacheMode {
		case EmptyResCacheMode__DELETE:
			{
				c.Logger().Debug("get empty result,delete old cache")
				err := c.Delete(ctx1)
				if err != nil {
					c.Logger().Warn("delete key get error", map[string]any{"err": err.Error()})
				}
			}
		case EmptyResCacheMode__IGNORE:
			{
				c.Logger().Debug("get empty result,refresh ttl")
				err := c.RefreshTTL(ctx1)
				if err != nil {
					c.Logger().Warn("efreshTT key get error", map[string]any{"err": err.Error()})
				}
			}
		case EmptyResCacheMode__SAVE:
			{
				c.Logger().Debug("get empty result,also save")
				c.saveToCache(ctx1, res)
			}
		}
	}
}

//AutoUpdate 自动更新缓存,自动更新缓存内容也受限流器影响,同时锁会用于限制更新程序的执行
func (c *Cache) AutoUpdate() error {
	if c.opt.UpdatePeriod == "" {
		return ErrAutoUpdateNeedUpdatePeriod
	}
	if c.c != nil {
		return ErrAutoUpdateAlreadyStarted
	}
	c.c = cron.New()
	c.c.AddFunc(c.opt.UpdatePeriod, c.ActiveUpdate)
	c.c.Start()
	return nil
}

//StopAutoUpdate 取消自动更新缓存
func (c *Cache) StopAutoUpdate() error {
	if c.opt.UpdatePeriod == "" || c.c == nil {
		return errors.New("auto update not start")
	}
	c.c.Stop()
	c.c = nil
	return nil
}

//lazyUpdate 被动加载新数据
func (c *Cache) lazyUpdate(ctx context.Context, force ForceLevelType) ([]byte, error) {
	res, reserr := c.update(force)
	if reserr != nil {
		return nil, reserr
	}
	//
	callback := func(ctx context.Context, res []byte) {
		//锁限制重复写入缓存
		if c.opt.Lock != nil {
			ctx1, cancel := context.WithTimeout(context.Background(), c.opt.QueryLockTimeout)
			defer cancel()
			err := c.opt.Lock.Lock(ctx1)
			if err != nil {
				if err == lock.ErrAlreadyLocked {
					c.Logger().Error("cache locked")
					return
				} else {
					c.Logger().Error("cache's lock get error", map[string]any{"err": err.Error()})
				}
			}
			defer c.opt.Lock.Unlock(ctx1)
		}
		c.saveToCache(ctx, res)
	}

	if len(res) > 0 {
		//获取的数据不为空
		//更新数据到缓存,如果有设置锁,只有缓存到redis后才会释放锁
		go callback(ctx, res)
	} else {
		//获取的数据为空
		switch c.opt.EmptyResCacheMode {
		case EmptyResCacheMode__DELETE:
			{
				c.Logger().Debug("get empty result,delete old cache")
				go func() {
					err := c.Delete(ctx)
					if err != nil {
						c.Logger().Warn("delete key get error", map[string]any{"err": err.Error()})
					}
				}()
			}
		case EmptyResCacheMode__IGNORE:
			{
				c.Logger().Debug("get empty result,refresh ttl")
				go func() {
					if c.MaxTTL() > 0 {
						err := c.RefreshTTL(ctx)
						if err != nil {
							c.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
						}
					}
				}()

			}
		case EmptyResCacheMode__SAVE:
			{
				c.Logger().Debug("get empty result,also save")
				go callback(ctx, res)
			}
		}

	}
	return res, nil
}

// 读操作

type GetOptions struct {
	Force ForceLevelType
}

var defaultGetOptions = GetOptions{
	Force: ForceLevel__NOCONSTRAINT,
}

//NoConstraintMode 设置使用无约束模式获取数据,无约束模式下无视组件处理,当更新函数得到的结果为空时不会放入缓存,当更新函数得到的结果为空时依然存入作为缓存
func NoConstraintMode() optparams.Option[GetOptions] {
	return optparams.NewFuncOption(func(o *GetOptions) {
		o.Force = ForceLevel__NOCONSTRAINT
	})
}

//StrictMode 设置使用严格模式获取数据,严格模式下无论如何只要报错和不满足组件要求就会终止,当更新函数得到的结果为空时不会放入缓存,而是刷新之前的过期时间
func StrictMode() optparams.Option[GetOptions] {
	return optparams.NewFuncOption(func(o *GetOptions) {
		o.Force = ForceLevel__STRICT
	})
}

//ConstraintMode 设置使用约束模式获取数据,约束模式下组件自身失效会继续处理,当更新函数得到的结果为空时会删除缓存以便下次再执行更新操作
func ConstraintMode() optparams.Option[GetOptions] {
	return optparams.NewFuncOption(func(o *GetOptions) {
		o.Force = ForceLevel__CONSTRAINT
	})
}

//Get 获取数据
//如果缓存中有就从缓存中获取,如果没有则直接从注册的缓存函数中获取,然后将结果更新到缓存
//如果cache的key有设置MaxTTL则在获取到缓存的数据是会刷新key的过期时间
//@param opt ...optparams.Option[GetOptions] 获取模式设置
func (c *Cache) Get(ctx context.Context, opt ...optparams.Option[GetOptions]) ([]byte, error) {
	p := defaultGetOptions
	optparams.GetOption(&p, opt...)
	ress, err := c.Client().Get(ctx, c.Key()).Result()
	if err != nil {
		res, err1 := c.lazyUpdate(ctx, p.Force)
		if err1 != nil {
			log.Debug("从函数获取失败")
			return nil, err1
		}
		if err == redis.Nil {
			log.Debug("未获得缓存")
			return res, nil
		}
		log.Debug("从函数获取成功")
		return res, nil
	}
	log.Debug("从缓存成功获取")
	go func() {
		if c.MaxTTL() > 0 {
			err := c.RefreshTTL(ctx)
			if err != nil {
				c.Logger().Warn("RefreshTTL key get error", map[string]any{"err": err.Error()})
			}
		}
	}()
	return []byte(ress), nil
}
