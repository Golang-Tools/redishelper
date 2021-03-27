//Package cache 缓存对象
//更新缓存往往是竞争更新,因此需要使用分布式锁避免重复计算,同时等待更新完成后再取数据.
package cache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"

	log "github.com/Golang-Tools/loggerhelper"

	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/lock"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//Cachefunc 缓存函数结果
type Cachefunc func() ([]byte, error)

//Cache 缓存
//缓存对象的锁可以和缓存不使用同一个redis客户端,甚至可以不用redis,只要他满足Canlock接口
//缓存设置UpdatePeriod后会自动定时同步数据
type Cache struct {
	latestHash string     //上次跟新后保存数据的hash,用于避免重复更新
	updateFunc Cachefunc  //更新缓存的函数
	c          *cron.Cron //定时任务对象
	*clientkey.ClientKey
	opt Options
}

//New 创建一个缓存实例
//updatePeriod会取最后设置的非空字符串
func New(k *clientkey.ClientKey, opts ...Option) *Cache {
	cache := new(Cache)
	cache.ClientKey = k
	cache.opt = Defaultopt
	for _, opt := range opts {
		opt.Apply(&cache.opt)
	}
	return cache
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
func (c *Cache) Update(ctx context.Context, force ForceLevelType) ([]byte, error) {
	var res []byte
	var reserr error
	if force == ForceLevel__NOCONSTRAINT {
		res, reserr = c.updateFunc()
	} else {
		//锁限制重复执行
		if c.opt.Lock != nil {
			err := c.opt.Lock.Lock(ctx)
			if err != nil {
				if err == lock.ErrAlreadyLocked {
					return nil, err
				}
				if force == ForceLevel__STRICT {
					return nil, err
				}
				log.Error("分布式锁错误", log.Dict{"err": err.Error()})
			}
		}
		//限制器限制执行防止被击穿
		if c.opt.Limiter != nil {
			canflood, err := c.opt.Limiter.Flood(ctx, 1)
			if err != nil {
				if force == ForceLevel__STRICT {
					return nil, err
				}
				log.Error("限制器报错", log.Dict{"err": err.Error()})
			}
			if !canflood {
				return nil, ErrLimiterNotAllow
			}
			res, reserr = c.updateFunc()
		} else {
			res, reserr = c.updateFunc()
		}
	}

	if reserr != nil {
		return nil, reserr
	}
	callback := func(ctx context.Context, res []byte) {
		if c.opt.Lock != nil {
			defer c.opt.Lock.Unlock(ctx)
		}
		h := md5.New()
		h.Write(res)
		resMd5 := hex.EncodeToString(h.Sum(nil))
		if c.latestHash == "" {
			_, err := c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Debug("设置缓存报错", log.Dict{"err": err.Error()})
				return
			}
			log.Debug("设置缓存成功")
			c.latestHash = resMd5
			return
		}
		if c.latestHash != resMd5 {
			_, err := c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Debug("设置缓存报错", log.Dict{"err": err.Error()})
				return
			}
			log.Debug("设置缓存成功")
			c.latestHash = resMd5
			return
		}
		if c.Opt.MaxTTL != 0 {
			log.Debug("结果未更新,刷新过期时间")
			_, err := c.Client.Expire(ctx, c.Key, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Debug("设置过期时间报错", log.Dict{"err": err.Error()})
				return
			}
			log.Debug("设置过期时间成功")
		}
		return
	}

	if res != nil && len(res) > 0 {
		//获取的数据不为空
		//更新数据到缓存,如果有设置锁,只有缓存到redis后才会释放锁
		go callback(ctx, res)
	} else {
		//获取的数据为空
		switch c.opt.EmptyResCacheMode {
		case EmptyResCacheMode__DELETE:
			{
				go func() {
					if c.Opt.MaxTTL != 0 {
						log.Debug("删除原有缓存")
						_, err := c.Client.Del(ctx, c.Key).Result()
						if err != nil {
							log.Debug("删除原有缓存报错", log.Dict{"err": err.Error()})
							return
						}
						log.Debug("删除原有缓存")
					}
				}()

			}
		case EmptyResCacheMode__IGNORE:
			{
				go func() {
					if c.Opt.MaxTTL != 0 {
						log.Debug("结果未更新,刷新过期时间")
						_, err := c.Client.Expire(ctx, c.Key, c.Opt.MaxTTL).Result()
						if err != nil {
							log.Debug("设置过期时间报错", log.Dict{"err": err.Error()})
							return
						}
						log.Debug("设置过期时间成功")
					}
				}()

			}
		case EmptyResCacheMode__SAVE:
			{
				go callback(ctx, res)
			}
		}

	}

	return res, nil
}

//AutoUpdate 自动更新缓存
func (c *Cache) AutoUpdate() error {
	if c.opt.UpdatePeriod == "" {
		return ErrAutoUpdateNeedUpdatePeriod
	}
	if c.c != nil {
		return ErrAutoUpdateAlreadyStarted
	}
	c.c = cron.New()
	c.c.AddFunc(c.opt.UpdatePeriod, func() {
		ctx := context.Background()
		_, err := c.Update(ctx, ForceLevel__STRICT)
		if err != nil {

		}
	})
	c.c.Start()
	return nil
}

//StopAutoUpdate 取消自动更新缓存
func (c *Cache) StopAutoUpdate() error {
	if c.opt.UpdatePeriod == "" || c.c == nil {
		return errors.New("自动更新未启动")
	}
	c.c.Stop()
	c.c = nil
	return nil
}

// 读操作

//Get 获取数据
//如果缓存中有就从缓存中获取,如果没有则直接从注册的缓存函数中获取,然后将结果更新到缓存
//如果cache的key有设置MaxTTL则在获取到缓存的数据是会刷新key的过期时间
//@param force bool 是否强制获取数据
func (c *Cache) Get(ctx context.Context, force ForceLevelType) ([]byte, error) {
	ress, err := c.Client.Get(ctx, c.Key).Result()
	if err != nil {
		res, err1 := c.Update(ctx, force)
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
	go c.RefreshTTL(ctx)
	return []byte(ress), nil
}
