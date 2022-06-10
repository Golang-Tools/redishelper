//Package 双端队列对象
package queuehelper

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
)

type Options struct {
	BlockTime            time.Duration                              //queue结构使用的参数,用于设置每次拉取的阻塞时长
	ProducerConsumerOpts []optparams.Option[pchelper.Options]       //初始化pchelper的配置
	ClientIDOpts         []optparams.Option[clientIdhelper.Options] //初始化ClientID的配置
}

var defaultOptions = Options{
	BlockTime:            1000 * time.Millisecond,
	ProducerConsumerOpts: []optparams.Option[pchelper.Options]{},
	ClientIDOpts:         []optparams.Option[clientIdhelper.Options]{},
}

//withMetaConfigs 使用optparams.Option[clientIdhelper.Options]设置Meta字段
func c(opts ...optparams.Option[clientIdhelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ClientIDOpts == nil {
			o.ClientIDOpts = []optparams.Option[clientIdhelper.Options]{}
		}
		o.ClientIDOpts = append(o.ClientIDOpts, opts...)
	})
}

//WithClientID 中间件通用设置,设置客户端id
func WithClientID(clientID string) optparams.Option[Options] {
	return c(clientIdhelper.WithClientID(clientID))
}

//PC withProducerConsumerConfigs的简写
func pc(opts ...optparams.Option[pchelper.Options]) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ProducerConsumerOpts == nil {
			o.ProducerConsumerOpts = []optparams.Option[pchelper.Options]{}
		}
		o.ProducerConsumerOpts = append(o.ProducerConsumerOpts, opts...)
	})
}

//SerializeWithJSON 使用JSON作为序列化反序列化的协议
func SerializeWithJSON() optparams.Option[Options] {
	return pc(pchelper.SerializeWithJSON())
}

//SerializeWithMsgpack 使用JSON作为序列化反序列化的协议
func SerializeWithMsgpack() optparams.Option[Options] {
	return pc(pchelper.SerializeWithMsgpack())
}

//WithUUIDSonyflake 使用sonyflake作为uuid的生成器
func WithUUIDSonyflake() optparams.Option[Options] {
	return pc(pchelper.WithUUIDSonyflake())
}

//WithUUIDSnowflake 使用snowflake作为uuid的生成器
func WithUUIDSnowflake() optparams.Option[Options] {
	return pc(pchelper.WithUUIDSnowflake())
}

//WithUUIDv4 使用uuid4作为uuid的生成器
func WithUUIDv4() optparams.Option[Options] {
	return pc(pchelper.WithUUIDv4())
}

//WithBlockTime 设置客户端阻塞等待消息的时长
func WithBlockTime(d time.Duration) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.BlockTime = d
	})
}
