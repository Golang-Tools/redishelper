package redishelper

import "github.com/go-redis/redis/v8"

//GoRedisV8Client `github.com/go-redis/redis/v8`客户端的接口
type GoRedisV8Client interface {
	redis.Cmdable
	AddHook(hook redis.Hook)
	Close() error
}
