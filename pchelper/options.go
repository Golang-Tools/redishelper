package pchelper

import (
	"github.com/Golang-Tools/idgener"
	"github.com/Golang-Tools/optparams"
)

//Options broker的配置
type Options struct {
	SerializeProtocol SerializeProtocolType
	// ClientID          string
	UUIDType idgener.IDGENAlgorithm
	// BlockTime         time.Duration //stream和queue结构使用的参数,用于设置每次拉取的阻塞时长
	// RecvBatchSize     int64         //stream消费者专用,用于设定一次获取的消息批长度
	// Group             string        //stream消费者专用,用于设定客户端组
	// AckMode           AckModeType   //stream消费者专用,用于设定同步校验规则
	// DefaultStart      string        //stream消费者专用,用于设定默认的监听的起始位置
	// DefaultMaxLen     int64         //stream生产者专用,用于设置流的默认最长长度
	// DefaultStrict     bool          //stream生产者专用,用于设置流是否为严格模式
}

var Defaultopt = Options{
	SerializeProtocol: SerializeProtocol_JSON,
	UUIDType:          idgener.IDGEN_UUIDV4,
	// BlockTime:         1000 * time.Millisecond,
	// RecvBatchSize:     1,
	// Group:             "",
	// AckMode:           AckModeAckWhenGet,
}

//SerializeWithJSON 使用JSON作为序列化反序列化的协议
func SerializeWithJSON() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.SerializeProtocol = SerializeProtocol_JSON
	})
}

//SerializeWithMsgpack 使用JSON作为序列化反序列化的协议
func SerializeWithMsgpack() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.SerializeProtocol = SerializeProtocol_MSGPACK
	})
}

//WithUUIDSonyflake 使用sonyflake作为uuid的生成器
func WithUUIDSonyflake() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.UUIDType = idgener.IDGEN_SNOYFLAKE
	})
}

//WithUUIDSnowflake 使用snowflake作为uuid的生成器
func WithUUIDSnowflake() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.UUIDType = idgener.IDGEN_SNOWFLAKE
	})
}

//WithUUIDv4 使用uuid4作为uuid的生成器
func WithUUIDv4() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.UUIDType = idgener.IDGEN_UUIDV4
	})
}

// //WithBlockTime 设置客户端阻塞等待消息的时长
// func WithBlockTime(d time.Duration) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.BlockTime = d
// 	})
// }

// //WithStreamComsumerRecvBatchSize stream消费者专用,用于设定一次获取的消息批长度
// func WithStreamComsumerRecvBatchSize(size int64) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.RecvBatchSize = size
// 	})
// }

// //WithStreamComsumerGroupName stream消费者专用,用于设定客户端组
// func WithStreamComsumerGroupName(groupname string) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.Group = groupname
// 	})
// }

// //WithStreamComsumerAckMode stream消费者专用,用于设定同步校验规则
// func WithStreamComsumerAckMode(ack AckModeType) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.AckMode = ack
// 	})
// }

// //WithDefaultStartPosition stream消费者专用,用于设定默认消费起始位置,不设置则group设置为`$`,否则设置为`>`
// func WithDefaultStartPosition(flag string) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.DefaultStart = flag
// 	})
// }

// //WithDefaultStartAt stream消费者专用,用于设定默认消费起始时间
// func WithDefaultStartAt(t time.Time) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		n := t.UnixNano()
// 		ns := strconv.FormatInt(n, 10)
// 		o.DefaultStart = ns
// 	})
// }

// //WithDefaultStartLatest stream消费者专用,用于设定默认消费从最新的消息开始
// func WithDefaultStartLatest() optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.DefaultStart = ""
// 	})
// }

// //WithDefaultStartEarliest stream消费者专用,用于设定默认消费从最早的消息开始
// func WithDefaultStartEarliest() optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.DefaultStart = "0"
// 	})
// }

// //WithDefaultMaxLen stream生产者专用,用于设置流的默认最长长度
// func WithDefaultMaxLen(n int64) optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.DefaultMaxLen = n
// 	})
// }

// //WithDefaultStrict stream生产者专用,用于设置流是否为严格模式
// func WithDefaultStrict() optparams.Option[Options] {
// 	return optparams.NewFuncOption(func(o *Options) {
// 		o.DefaultStrict = true
// 	})
// }
