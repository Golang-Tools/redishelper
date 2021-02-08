package redishelper

import (
	"context"

	"github.com/Golang-Tools/redishelper/clientkey"
)

//CanBeCounter 计数器接口
type CanBeCounter interface {
	Add(context.Context, ...interface{}) error
	AddM(context.Context, ...interface{}) error
	Len(context.Context) (int64, error)
}

//CanBeSet 可以被看作时Set的结构对象
type CanBeSet interface {
	CanBeCounter
	Remove(context.Context, ...interface{}) error
	RemoveM(context.Context, ...interface{}) error
	Contained(context.Context, interface{}) (bool, error)
	ToArray(context.Context) ([]interface{}, error)
	Intersection(context.Context, *clientkey.ClientKey, ...CanBeSet) (CanBeSet, error)
	Union(context.Context, *clientkey.ClientKey, ...CanBeSet) (CanBeSet, error)
	Except(context.Context, *clientkey.ClientKey, CanBeSet) (CanBeSet, error)
	Xor(context.Context, *clientkey.ClientKey, CanBeSet) (CanBeSet, error)
}

//CanBeGenerator 生成器的接口
type CanBeGenerator interface {
	Next(context.Context) (int64, error)
}
