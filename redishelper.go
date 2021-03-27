package redishelper

import (
	"context"

	"github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey"
)

//CanBeCount 可以被计数
type CanBeCount interface {
	Len(context.Context) (int64, error)
	Reset(context.Context) error
}

//CanBeGenerator 生成器的接口
type CanBeGenerator interface {
	Next(context.Context) (int64, error)
}

//CanBeSet 可以被看作时Set的结构对象
type CanBeSet interface {
	CanBeCount
	Add(context.Context, ...interface{}) error
	AddM(context.Context, ...interface{}) error
	Remove(context.Context, ...interface{}) error
	RemoveM(context.Context, ...interface{}) error
	Contained(context.Context, interface{}) (bool, error)
	ToArray(context.Context) ([]interface{}, error)
	Intersection(context.Context, *clientkey.ClientKey, ...CanBeSet) (CanBeSet, error)
	Union(context.Context, *clientkey.ClientKey, ...CanBeSet) (CanBeSet, error)
	Except(context.Context, *clientkey.ClientKey, CanBeSet) (CanBeSet, error)
	Xor(context.Context, *clientkey.ClientKey, CanBeSet) (CanBeSet, error)
}

//CanBeCounter 计数器接口
type CanBeCounter interface {
	CanBeGenerator
	CanBeCount
}

//CanBeLimiter 限制器接口
type CanBeLimiter interface {
	//注水,返回值true表示注水成功,false表示满了无法注水,抛出异常返回true
	Flood(context.Context, int64) (bool, error)
	//水位
	WaterLevel(context.Context) (int64, error)
	//容量
	Capacity() int64
	//是否容量已满
	IsFull(context.Context) (bool, error)
	//重置
	Reset(context.Context) error
}

//Canlock 锁对象的接口
type Canlock interface {
	Lock(context.Context) error
	Unlock(context.Context) error
	Wait(context.Context) error
}

//CanBeConsumer  消费者对象的接口
type CanBeConsumer interface {
	RegistHandler(topic string, fn event.Handdler) error
	UnRegistHandler(topic string) error
	Listen(asyncHanddler bool, p ...event.Parser) error
	StopListening() error
}

//CanBeProducer 生产者对象的接口
type CanBeProducer interface {
	Publish(ctx context.Context, payload interface{}) error
	PubEvent(ctx context.Context, payload interface{}) (*event.Event, error)
}
