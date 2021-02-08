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
	parallelcallback bool
	callBacks        []Callback
}

// New 创建一个新的数据库客户端代理
func New() *redisProxy {
	proxy := new(redisProxy)
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *redisProxy) IsOk() bool {
	if proxy.UniversalClient == nil {
		return false
	}
	return true
}

//SetConnect 设置连接的客户端
//@params cli UniversalClient 满足redis.UniversalClient接口的对象的指针
func (proxy *redisProxy) SetConnect(cli redis.UniversalClient, hooks ...redis.Hook) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedUniversalClient
	}
	for _, hook := range hooks {
		cli.AddHook(hook)
	}
	proxy.UniversalClient = cli
	if proxy.parallelcallback {
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

//InitFromOptions 从配置条件初始化代理对象
func (proxy *redisProxy) InitFromOptions(options *redis.Options, hooks ...redis.Hook) error {
	cli := redis.NewClient(options)
	return proxy.SetConnect(cli, hooks...)
}

//InitFromOptionsParallelCallback 从配置条件初始化代理对象,并行执行回调函数
func (proxy *redisProxy) InitFromOptionsParallelCallback(options *redis.Options, hooks ...redis.Hook) error {
	cli := redis.NewClient(options)
	proxy.parallelcallback = true
	return proxy.SetConnect(cli, hooks...)
}

//InitFromURL 从URL条件初始化代理对象
func (proxy *redisProxy) InitFromURL(url string, hooks ...redis.Hook) error {
	options, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	return proxy.InitFromOptions(options, hooks...)
}

//InitFromURLParallelCallback 从URL条件初始化代理对象
func (proxy *redisProxy) InitFromURLParallelCallback(url string, hooks ...redis.Hook) error {
	options, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	return proxy.InitFromOptionsParallelCallback(options, hooks...)
}

//InitFromClusterOptions 从集群设置条件初始化代理对象
func (proxy *redisProxy) InitFromClusterOptions(options *redis.ClusterOptions, hooks ...redis.Hook) error {
	cli := redis.NewClusterClient(options)
	return proxy.SetConnect(cli, hooks...)
}

//InitFromClusterOptions 从集群设置条件初始化代理对象
func (proxy *redisProxy) InitFromClusterOptionsParallelCallback(options *redis.ClusterOptions, hooks ...redis.Hook) error {
	cli := redis.NewClusterClient(options)
	proxy.parallelcallback = true
	return proxy.SetConnect(cli, hooks...)
}

//InitFromFailoverOptions 从集群设置条件初始化代理对象
func (proxy *redisProxy) InitFromFailoverOptions(options *redis.FailoverOptions, hooks ...redis.Hook) error {
	cli := redis.NewFailoverClusterClient(options)
	return proxy.SetConnect(cli, hooks...)
}

//InitFromFailoverOptions从集群设置条件初始化代理对象
func (proxy *redisProxy) InitFromFailoverOptionsParallelCallback(options *redis.FailoverOptions, hooks ...redis.Hook) error {
	cli := redis.NewFailoverClusterClient(options)
	proxy.parallelcallback = true
	return proxy.SetConnect(cli, hooks...)
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
