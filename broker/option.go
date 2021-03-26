package broker

import (
	"github.com/Golang-Tools/redishelper/randomkey"
)

//Options broker的配置
type Options struct {
	SerializeProtocol string
	ClientID          uint16
	UUIDType          string
}

var Defaultopt = Options{
	SerializeProtocol: "JSON",
	ClientID:          randomkey.GetMachineID(),
	UUIDType:          "sonyflake",
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

// type emptyOption struct{}

// func (emptyOption) apply(*Options) {}

type funcOption struct {
	f func(*Options)
}

func (fo *funcOption) Apply(do *Options) {
	fo.f(do)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

//SerializeWithJSON 使用JSON作为序列化反序列化的协议
func SerializeWithJSON() Option {
	return newFuncOption(func(o *Options) {
		o.SerializeProtocol = "JSON"
	})
}

//SerializeWithMsgpack 使用JSON作为序列化反序列化的协议
func SerializeWithMsgpack() Option {
	return newFuncOption(func(o *Options) {
		o.SerializeProtocol = "msgpack"
	})
}

//WithClientID 设置
func WithClientID(clientID uint16) Option {
	return newFuncOption(func(o *Options) {
		o.ClientID = clientID
	})
}

//WithUUIDSonyflake 使用sonyflake作为uuid的生成器
func WithUUIDSonyflake() Option {
	return newFuncOption(func(o *Options) {
		o.UUIDType = "sonyflake"
	})
}

//WithUUIDv4 使用uuid4作为uuid的生成器
func WithUUIDv4() Option {
	return newFuncOption(func(o *Options) {
		o.UUIDType = "uuidv4"
	})
}
