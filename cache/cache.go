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
//每次更新会判断是否有变化,没有变化如果设置有MAXTTL则只刷新过期时间,有变化则会更新变化并刷新过期时间
func (c *Cache) Update(ctx context.Context) ([]byte, error) {
	res, err := c.updateFunc()
	if err != nil {
		return nil, err
	}

	go func(ctx context.Context, res []byte) {
		err := c.opt.Lock.Lock(ctx)
		if err != nil {
			if err == lock.ErrAlreadyLocked {
				log.Debug("缓存已被锁定")
				return
			}
			log.Debug("获得分布式锁错误", log.Dict{"err": err.Error()})
			c.opt.Lock.Unlock(ctx)
			return
		}
		defer c.opt.Lock.Unlock(ctx)
		h := md5.New()
		h.Write(res)
		resMd5 := hex.EncodeToString(h.Sum(nil))
		if c.latestHash == "" {
			_, err = c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Debug("设置缓存报错", log.Dict{"err": err.Error()})
				return
			}
			log.Debug("设置缓存成功")
			c.latestHash = resMd5
			return
		}
		if c.latestHash != resMd5 {
			_, err = c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
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
			_, err = c.Client.Expire(ctx, c.Key, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Debug("设置过期时间报错", log.Dict{"err": err.Error()})
				return
			}
			log.Debug("设置过期时间成功")
		}
		return
	}(ctx, res)
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
		_, err := c.Update(ctx)
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
func (c *Cache) Get(ctx context.Context) ([]byte, error) {
	ress, err := c.Client.Get(ctx, c.Key).Result()
	if err != nil {
		res, err1 := c.Update(ctx)
		if err1 != nil {
			log.Debug("从函数获取失败")
			return nil, err1
		}
		if err == redis.Nil {
			log.Debug("未获得缓存")
			return res, nil
		}
		log.Debug("从函数获取成功")
		return res, err
	}
	log.Debug("从缓存成功获取")
	err = c.RefreshTTL(ctx)
	if err != nil && err != clientkey.ErrKeyNotSetMaxTLL {
		log.Debug("未刷新缓存")
		return []byte(ress), err
	}
	log.Debug("刷新缓存成功")
	return []byte(ress), nil
}
