package redishelper

import (
	log "github.com/Basic-Components/loggerhelper"

	"sync"

	"github.com/go-redis/redis/v7"
)

// redisHelperCallback redis操作的回调函数
type redisHelperCallback func(cli *redis.Client) error

// redisHelper redis客户端的代理
type redisHelper struct {
	helperLock sync.RWMutex //代理的锁
	Options    *redis.Options
	conn       *redis.Client
	callBacks  []redisHelperCallback
}

// New 创建一个新的数据库客户端代理
func New() *redisHelper {
	proxy := new(redisHelper)
	proxy.helperLock = sync.RWMutex{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *redisHelper) IsOk() bool {
	if proxy.conn == nil {
		return false
	}
	return true
}

func (proxy *redisHelper) GetConn() (*redis.Client, error) {
	if !proxy.IsOk() {
		return proxy.conn, ErrHelperNotInited
	}
	proxy.helperLock.RLock()
	defer proxy.helperLock.RUnlock()
	return proxy.conn, nil
}

// Close 关闭pg
func (proxy *redisHelper) Close() {
	if proxy.IsOk() {
		proxy.conn.Close()
		proxy.helperLock.Lock()
		proxy.conn = nil
		proxy.helperLock.Unlock()
	}
}

//SetConnect 设置连接的客户端
func (proxy *redisHelper) SetConnect(cli *redis.Client) {
	proxy.helperLock.Lock()
	proxy.conn = cli
	proxy.helperLock.Unlock()
	for _, cb := range proxy.callBacks {
		err := cb(proxy.conn)
		if err != nil {
			log.Error(map[string]interface{}{"err": err}, "regist callback get error")
		} else {
			log.Info(nil, "regist callback done")
		}
	}
}

// Init 给代理赋值客户端实例
func (proxy *redisHelper) Init(cli *redis.Client) error {
	if proxy.IsOk() {
		return ErrHelperAlreadyInited
	}
	proxy.SetConnect(cli)
	return nil
}

// InitFromOptions 从配置条件初始化代理对象
func (proxy *redisHelper) InitFromOptions(options *redis.Options) error {
	proxy.Options = options
	cli := redis.NewClient(options)
	return proxy.Init(cli)
}

// InitFromURL 从URL条件初始化代理对象
func (proxy *redisHelper) InitFromURL(url string) error {
	options, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	return proxy.InitFromOptions(options)
}

// Regist 注册回调函数,在init执行后执行回调函数
func (proxy *redisHelper) Regist(cb redisHelperCallback) {
	proxy.callBacks = append(proxy.callBacks, cb)
}

//NewLock 创建一个全局锁
func (proxy *redisHelper) NewLock(key string, timeout int64) *distributedLock {
	lock := newLock(proxy, key, timeout)
	return lock
}

//NewCounter 创建一个全局锁
func (proxy *redisHelper) NewCounter(key string) *distributedcounter {
	counter := newCounter(proxy, key)
	return counter
}

//NewBitmap 创建一个位图
func (proxy *redisHelper) NewBitmap(key string) *bitmap {
	bm := newBitmap(proxy, key)
	return bm
}

//NewStreamTopic 创建一个流的主题对象
func (proxy *redisHelper) NewStreamTopic(topic string, maxlen int64, strict bool) *StreamTopic {
	bm := NewStreamTopic(proxy, topic, maxlen, strict)
	return bm
}

//NewStreamProducer 创建一个流的生产者对象
func (proxy *redisHelper) NewStreamProducer(topic string, maxlen int64, strict bool) *streamProducer {
	bm := newStreamProducer(proxy, topic, maxlen, strict)
	return bm
}

//NewStreamConsumer 创建一个流的生产者对象
func (proxy *redisHelper) NewStreamConsumer(topics []string, start string, count int64, block int64, name string, noack bool, group ...string) *streamConsumer {
	bm := newStreamConsumer(proxy, topics, start, count, block, name, noack, group...)
	return bm
}

//NewPubSubTopic 创建一个流的主题对象
func (proxy *redisHelper) NewPubSubTopic(topic string) *PubSubTopic {
	bm := NewPubSubTopic(proxy, topic)
	return bm
}

//NewPubSubProducer 创建一个流的生产者对象
func (proxy *redisHelper) NewPubSubProducer(topic string) *pubsubProducer {
	bm := newPubSubProducer(proxy, topic)
	return bm
}

//NewStreamConsumer 创建一个流的生产者对象
func (proxy *redisHelper) NewPubSubConsumer(topics []string) *pubsubConsumer {
	bm := newPubSubConsumer(proxy, topics)
	return bm
}

//NewQueueTopic 创建一个流的主题对象
func (proxy *redisHelper) NewQueue(topic string) *Queue {
	bm := NewQueue(proxy, topic)
	return bm
}

//NewPubSubProducer 创建一个流的生产者对象
func (proxy *redisHelper) NewQueueProducer(topic string) *queueProducer {
	bm := newQueueProducer(proxy, topic)
	return bm
}

//NewStreamConsumer 创建一个流的生产者对象
func (proxy *redisHelper) NewQueueConsumer(topics []string) *queueConsumer {
	bm := newQueueConsumer(proxy, topics)
	return bm
}

//NewRanker 创建一个排名器
func (proxy *redisHelper) NewRanker(name string) *ranker {
	bm := newRanker(proxy, name)
	return bm
}

// Helper 默认的pg代理对象
var Helper = New()
