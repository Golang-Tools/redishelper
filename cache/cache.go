package cache

//更新缓存往往是竞争更新,因此需要使用分布式锁避免重复计算,同时等待更新完成后再取数据.
import (
	"context"
	"errors"
	"time"

	"github.com/Golang-Tools/redishelper/lock"
	"github.com/go-redis/redis/v8"
)

//MiniUpdatePeriod 主动更新间隔最低60s
const MiniUpdatePeriod = 60 * time.Second

//ErrArgUpdatePeriodMoreThan1 UpdatePeriod参数的个数超过1个
var ErrArgUpdatePeriodMoreThan1 = errors.New(" updatePeriod 必须只有1位或者没有设置")

//ErrUpdatePeriodLessThan60Second UpdatePeriod小于100微秒
var ErrUpdatePeriodLessThan60Second = errors.New(" updatePeriod 必须不小于60秒")

//Cachefunc 缓存函数结果
type Cachefunc func() []byte

//Cache 缓存
type Cache struct {
	Key          string                //缓存使用的key
	MaxTTL       time.Duration         //缓存的最大过期时间
	UpdatePeriod time.Duration         //间隔多久主动更新,位0则不主动更新
	lock         lock.Canlock          //使用的锁
	client       redis.UniversalClient //redis客户端对象
	latestHash   []byte                //上次跟新后保存数据的hash,用于避免重复更新
	updateFunc   Cachefunc             //更新缓存的函数
}

//New 创建一个缓存实例
func New(key string, maxTTL time.Duration, lock lock.Canlock, client redis.UniversalClient, updatePeriod ...time.Duration) (*Cache, error) {
	cache := new(Cache)
	cache.Key = key
	cache.MaxTTL = maxTTL
	cache.lock = lock
	cache.client = client
	switch len(updatePeriod) {
	case 0:
		{
			return cache, nil
		}
	case 1:
		{
			cp := updatePeriod[0]
			if cp < MiniUpdatePeriod {
				return nil, ErrUpdatePeriodLessThan60Second
			}
			cache.UpdatePeriod = cp
			return cache, nil
		}
	default:
		{
			return nil, ErrArgUpdatePeriodMoreThan1
		}
	}

}
func (c *Cache) RegistUpdateFunc(Cachefunc) error {

}

func (c *Cache) Get(ctx context.Context) error {

}

func (c *Cache) Update(ctx context.Context) error {

}
