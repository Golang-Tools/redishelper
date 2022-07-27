package middlewarehelper

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/go-redis/redis/v8"
)

//MiddlewareInterface 一个对象满足该接口表示它是一个redis构造的中间件
type MiddlewareInterface interface {
	//Cli 获取中间件使用的redis客户端
	Cli() redis.UniversalClient
	//Key 查看中间件使用的redis键
	Key() string
	//MiddlewareType 查看中间件类型,有lock,limiter,cache,counter,set,disctinct_counter,producer,consumer
	MiddlewareType() string
	//Exists 查看key是否存在
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Exists(ctx context.Context) (bool, error)
	//Type 查看key的类型
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Type(ctx context.Context) (string, error)
	//MaxTTL 查看组件设置的最大过期时间
	MaxTTL() time.Duration
	//Logger 获取中间件对象的专用log
	Logger() *log.Log
	//TTL 查看key的剩余时间
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	TTL(ctx context.Context) (time.Duration, error)
	//写操作
	//Delete 删除key
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	Delete(ctx context.Context) error
	//RefreshTTL 刷新key的生存时间
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	RefreshTTL(ctx context.Context) error
	//AutoRefresh 自动刷新key的过期时间
	AutoRefresh() error
	//StopAutoRefresh 取消自动更新缓存
	//@params opts ...optparams.Option[ForceOpt] 使用参数Force()强制停下整个定时任务cron对象
	StopAutoRefresh(opts ...optparams.Option[ForceOpt]) error

	//DoCmdWithTTL 单条命令改为pipeline执行后添加ttl
	//@params ctx context.Context 上下文信息,用于控制请求的结束
	//@params cmd []interface{} 待执行命令
	//@params exp time.Duration ttl超时
	//@returns interface{} 命令执行好后调用`.Result()`返回的值
	DoCmdWithTTL(ctx context.Context, cmd []interface{}, exp time.Duration) (interface{}, error)
}
