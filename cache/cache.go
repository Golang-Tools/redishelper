//Package cache 缓存对象
//更新缓存往往是竞争更新,因此需要使用分布式锁避免重复计算,同时等待更新完成后再取数据.
package cache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"time"

	log "github.com/Golang-Tools/loggerhelper"

	h "github.com/Golang-Tools/redishelper"
	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/lock"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

//MiniUpdatePeriod 主动更新间隔最低60s
const MiniUpdatePeriod = 60 * time.Second

//Cachefunc 缓存函数结果
type Cachefunc func() ([]byte, error)

//Cache 缓存
type Cache struct {
	UpdatePeriod string     //间隔多久主动更新,为空字符串则不主动更新,使用crontab格式
	lock         h.Canlock  //使用的锁
	latestHash   string     //上次跟新后保存数据的hash,用于避免重复更新
	updateFunc   Cachefunc  //更新缓存的函数
	c            *cron.Cron //定时任务对象
	*clientkey.ClientKey
}

//New 创建一个缓存实例
func New(k *clientkey.ClientKey, lock h.Canlock, updatePeriod ...string) (*Cache, error) {
	cache := new(Cache)
	cache.ClientKey = k
	cache.lock = lock
	switch len(updatePeriod) {
	case 0:
		{
			return cache, nil
		}
	case 1:
		{
			cp := updatePeriod[0]
			cache.UpdatePeriod = cp
			return cache, nil
		}
	default:
		{
			return nil, ErrArgUpdatePeriodMoreThan1
		}
	}

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
func (c *Cache) Update(ctx context.Context) ([]byte, error) {
	res, err := c.updateFunc()
	if err != nil {
		return nil, err
	}

	go func(ctx context.Context, res []byte) {
		err := c.lock.Lock(ctx)

		if err != nil {
			if err == lock.ErrAlreadyLocked {
				log.Warn("缓存已被锁定")
				return
			}
			log.Error("获得分布式锁错误", log.Dict{"err": err.Error()})
			return
		}
		defer c.lock.Unlock(ctx)
		h := md5.New()
		h.Write(res)
		resMd5 := hex.EncodeToString(h.Sum(nil))
		if c.latestHash == "" {
			_, err = c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Error("设置缓存报错", log.Dict{"err": err.Error()})
				return
			}
			log.Info("设置缓存成功")
			c.latestHash = resMd5
			return
		}
		if c.latestHash != resMd5 {
			_, err = c.Client.Set(ctx, c.Key, res, c.Opt.MaxTTL).Result()
			if err != nil {
				log.Error("设置缓存报错", log.Dict{"err": err.Error()})
				return
			}
			log.Info("设置缓存成功")
			c.latestHash = resMd5
			return
		}
		log.Warn("结果未更新,刷新过期时间")
		_, err = c.Client.Expire(ctx, c.Key, c.Opt.MaxTTL).Result()
		if err != nil {
			log.Error("设置过期时间报错", log.Dict{"err": err.Error()})
			return
		}
		log.Info("设置过期时间成功")
		return
	}(ctx, res)

	return res, nil
}

//AutoUpdate 自动更新缓存
func (c *Cache) AutoUpdate() error {
	if c.UpdatePeriod == "" {
		return ErrAutoUpdateNeedUpdatePeriod
	}
	if c.c != nil {
		return ErrAutoUpdateAlreadyStarted
	}
	c.c = cron.New()
	c.c.AddFunc(c.UpdatePeriod, func() {
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
	if c.UpdatePeriod == "" || c.c == nil {
		return errors.New("自动更新未启动")
	}
	c.c.Stop()
	c.c = nil
	return nil
}

// 读操作

//Get 获取数据,如果缓存中有就从缓存中获取,如果没有则直接从注册的缓存函数中获取,然后将结果更新到缓存
func (c *Cache) Get(ctx context.Context) ([]byte, error) {
	ress, err := c.Client.Get(ctx, c.Key).Result()
	if err != nil {
		if err == redis.Nil {
			res, err := c.Update(ctx)
			if err != nil {
				return nil, err
			}
			return res, nil
		}
		return nil, err
	}
	return []byte(ress), nil
}
