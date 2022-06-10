package redisproxy

import (
	"time"

	"github.com/Golang-Tools/optparams"
	redis "github.com/go-redis/redis/v8"
)

// redis类型
type RedisType int32

const (
	Redis_Standalone      RedisType = 0
	Redis_Cluster         RedisType = 1
	Redis_Failover        RedisType = 2
	Redis_FailoverCluster RedisType = 3
	Redis_Ring            RedisType = 4
)

//Option 设置key行为的选项
type Options struct {
	Type              RedisType
	Parallelcallback  bool
	QueryTimeout      time.Duration
	StandAloneOptions *redis.Options
	ClusterOptions    *redis.ClusterOptions
	FailoverOptions   *redis.FailoverOptions
	RingOptions       *redis.RingOptions
	Hooks             []redis.Hook
}

var DefaultOpts = Options{
	StandAloneOptions: &redis.Options{Addr: "localhost:6379"},
	Type:              Redis_Standalone,
	Parallelcallback:  false,
	Hooks:             []redis.Hook{},
}

//WithQueryTimeoutMS 设置最大请求超时,单位ms
func WithQueryTimeoutMS(QueryTimeout int) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.QueryTimeout = time.Duration(QueryTimeout) * time.Millisecond
	})
}

//WithParallelCallback 设置初始化后回调并行执行而非串行执行
func WithParallelCallback() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Parallelcallback = true
	})
}

//WithHooks 增加redis钩子
func WithHooks(hooks ...redis.Hook) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.Hooks == nil {
			o.Hooks = []redis.Hook{}
		}
		o.Hooks = append(o.Hooks, hooks...)
	})
}

//WithOptions 使用特定单机redis连接设置
func WithOptions(RedisOptions *redis.Options) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Type = Redis_Standalone
		o.StandAloneOptions = RedisOptions
	})
}

//WithURL 使用特定url设置单机redis的连接
func WithURL(URL string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		RedisOptions, err := redis.ParseURL(URL)
		if err != nil {
			logger.Warn("redis url can not parse", map[string]any{"url": URL})
		} else {
			o.Type = Redis_Standalone
			o.StandAloneOptions = RedisOptions
		}
	})
}

//WithClusterOptions 使用特定redis集群连接设置
func WithClusterOptions(RedisOptions *redis.ClusterOptions) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Type = Redis_Cluster
		o.ClusterOptions = RedisOptions
	})
}

//WithFailoverOptions 使用特定redis哨兵连接设置
func WithFailoverOptions(RedisOptions *redis.FailoverOptions) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Type = Redis_Failover
		o.FailoverOptions = RedisOptions
	})
}

//WithFailoverOptions 使用特定redis集群哨兵连接设置
func WithFailoverClusterOptions(RedisOptions *redis.FailoverOptions) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Type = Redis_FailoverCluster
		o.FailoverOptions = RedisOptions
	})
}

//WithRingOptions 使用特定redis的Ring集群连接设置
func WithRingOptions(RedisOptions *redis.RingOptions) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Type = Redis_Ring
		o.RingOptions = RedisOptions
	})
}
