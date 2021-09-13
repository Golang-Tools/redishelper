package proxy

import (
	log "github.com/Golang-Tools/loggerhelper"
	redis "github.com/go-redis/redis/v8"
)

//Callback redis操作的回调函数
type Callback func(cli redis.UniversalClient) error

//redisProxy redis客户端的代理
type redisProxy struct {
	redis.UniversalClient
	opts      Options
	callBacks []Callback
}

// New 创建一个新的数据库客户端代理
func New() *redisProxy {
	proxy := new(redisProxy)
	proxy.opts = DefaultOpts
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *redisProxy) IsOk() bool {
	return proxy.UniversalClient != nil
}

//SetConnect 设置连接的客户端
//@params cli UniversalClient 满足redis.UniversalClient接口的对象的指针
func (proxy *redisProxy) SetConnect(cli redis.UniversalClient) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedUniversalClient
	}
	proxy.UniversalClient = cli
	if proxy.opts.Parallelcallback {
		for _, cb := range proxy.callBacks {
			go func(cb Callback) {
				err := cb(proxy.UniversalClient)
				if err != nil {
					log.Error("regist callback get error", log.Dict{"err": err})
				} else {
					log.Debug("regist callback done")
				}
			}(cb)
		}
	} else {
		for _, cb := range proxy.callBacks {
			err := cb(proxy.UniversalClient)
			if err != nil {
				log.Error("regist callback get error", log.Dict{"err": err})
			} else {
				log.Debug("regist callback done")
			}
		}
	}
	return nil
}

func (proxy *redisProxy) Init(opts ...Option) error {
	for _, opt := range opts {
		opt.Apply(&proxy.opts)
	}
	var cli redis.UniversalClient
	switch proxy.opts.Type {
	case Redis_Standalone:
		{
			cli = redis.NewClient(proxy.opts.StandAloneOptions)

		}
	case Redis_Cluster:
		{
			cli = redis.NewClusterClient(proxy.opts.ClusterOptions)
		}
	case Redis_Failover:
		{
			cli = redis.NewFailoverClient(proxy.opts.FailoverOptions)
		}
	case Redis_FailoverCluster:
		{
			cli = redis.NewFailoverClusterClient(proxy.opts.FailoverOptions)
		}
	case Redis_Ring:
		{
			cli = redis.NewRing(proxy.opts.RingOptions)
		}
	default:
		{
			return ErrUnknownClientType
		}

	}
	if len(proxy.opts.Hooks) > 0 {
		for _, hook := range proxy.opts.Hooks {
			cli.AddHook(hook)
		}
	}
	return proxy.SetConnect(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
//如果对象已经设置了被代理客户端则无法再注册回调函数
func (proxy *redisProxy) Regist(cb Callback) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedUniversalClient
	}
	proxy.callBacks = append(proxy.callBacks, cb)
	return nil
}

//Proxy 默认的redis代理对象
var Proxy = New()
