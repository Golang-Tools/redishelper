//Package stream对象
package streamhelper

import (
	"strconv"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
)

//AckModeType stream的Ack模式
type AckModeType uint8

const (

	//AckModeAckWhenGet 获取到后确认
	AckModeAckWhenGet AckModeType = iota
	//AckModeAckWhenDone 处理完后确认
	AckModeAckWhenDone
	//AckModeNoAck 不做确认,消费者需要自己实现ack操作,最好别这么用
	AckModeNoAck
)

type Options struct {
	BlockTime            time.Duration                              //queue结构使用的参数,用于设置每次拉取的阻塞时长
	RecvBatchSize        int64                                      //stream消费者专用,用于设定一次获取的消息批长度
	Group                string                                     //stream消费者专用,用于设定客户端组
	AckMode              AckModeType                                //stream消费者专用,用于设定同步校验规则
	DefaultStart         string                                     //stream消费者专用,用于设定默认的监听的起始位置
	DefaultMaxLen        int64                                      //stream生产者专用,用于设置流的默认最长长度
	DefaultStrict        bool                                       //stream生产者专用,用于设置流是否为严格模式
	ProducerConsumerOpts []optparams.Option[pchelper.Options]       //初始化pchelper的配置
	ClientIDOpts         []optparams.Option[clientIdhelper.Options] //初始化ClientID的配置
}

var defaultOptions = Options{
	BlockTime:            1000 * time.Millisecond,
	ProducerConsumerOpts: []optparams.Option[pchelper.Options]{},
	ClientIDOpts:         []optparams.Option[clientIdhelper.Options]{},
	RecvBatchSize:        1,
	Group:                "",
	AckMode:              AckModeAckWhenGet,
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

//pc 使用optparams.Option[limiterhelper.Options]设置limiter配置
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

//WithConsumerRecvBatchSize stream消费者专用,用于设定一次获取的消息批长度
func WithConsumerRecvBatchSize(size int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.RecvBatchSize = size
	})
}

//WithConsumerGroupName stream消费者专用,用于设定客户端组
func WithConsumerGroupName(groupname string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.Group = groupname
	})
}

//WithConsumerAckMode stream消费者专用,用于设定同步校验规则
func WithConsumerAckMode(ack AckModeType) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.AckMode = ack
	})
}

//WithConsumerDefaultStartPosition stream消费者专用,用于设定默认消费起始位置,不设置则group设置为`$`,否则设置为`>`
func WithConsumerDefaultStartPosition(flag string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.DefaultStart = flag
	})
}

//WithConsumerDefaultStartAt stream消费者专用,用于设定默认消费起始时间
func WithConsumerDefaultStartAt(t time.Time) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		n := t.UnixNano()
		ns := strconv.FormatInt(n, 10)
		o.DefaultStart = ns
	})
}

//WithConsumerDefaultStartLatest stream消费者专用,用于设定默认消费从最新的消息开始
func WithConsumerDefaultStartLatest() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.DefaultStart = ""
	})
}

//WithConsumerDefaultStartEarliest stream消费者专用,用于设定默认消费从最早的消息开始
func WithConsumerDefaultStartEarliest() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.DefaultStart = "0"
	})
}

//WithProducerDefaultMaxLen stream生产者专用,用于设置流的默认最长长度
func WithProducerDefaultMaxLen(n int64) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.DefaultMaxLen = n
	})
}

//WithProducerStrict stream生产者专用,用于设置流是否为严格模式
func WithProducerDefaultStrict() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.DefaultStrict = true
	})
}
