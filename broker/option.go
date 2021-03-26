package broker

import (
	"strconv"
	"time"

	"github.com/Golang-Tools/redishelper/randomkey"
)

//AckModeType stream的Ack模式
type AckModeType uint16

const (

	//AckModeAckWhenGet 获取到后确认
	AckModeAckWhenGet AckModeType = iota
	//AckModeAckWhenDone 处理完后确认
	AckModeAckWhenDone
	//AckModeNoAck 不做确认,消费者需要自己实现ack操作,最好别这么用
	AckModeNoAck
)

//Options broker的配置
type Options struct {
	SerializeProtocol string
	ClientID          uint16
	UUIDType          string
	BlockTime         time.Duration //stream和queue结构使用的参数,用于设置每次拉取的阻塞时长
	RecvBatchSize     int64         //stream消费者专用,用于设定一次获取的消息批长度
	Group             string        //stream消费者专用,用于设定客户端组
	AckMode           AckModeType   //stream消费者专用,用于设定同步校验规则
	Start             string        //stream消费者专用,用于设定监听的起始位置,"则从最近的开始监听"
}

var Defaultopt = Options{
	SerializeProtocol: "JSON",
	ClientID:          randomkey.GetMachineID(),
	UUIDType:          "sonyflake",
	BlockTime:         100 * time.Millisecond,
	RecvBatchSize:     1,
	Group:             "",
	AckMode:           AckModeAckWhenGet,
	Start:             "",
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

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

//WithBlockTime 设置客户端阻塞等待消息的时长
func WithBlockTime(d time.Duration) Option {
	return newFuncOption(func(o *Options) {
		o.BlockTime = d
	})
}

//WithStreamComsumerRecvBatchSize stream消费者专用,用于设定一次获取的消息批长度
func WithStreamComsumerRecvBatchSize(size int64) Option {
	return newFuncOption(func(o *Options) {
		o.RecvBatchSize = size
	})
}

//WithStreamComsumerGroupName stream消费者专用,用于设定客户端组
func WithStreamComsumerGroupName(groupname string) Option {
	return newFuncOption(func(o *Options) {
		o.Group = groupname
	})
}

//WithStreamComsumerAckMode stream消费者专用,用于设定同步校验规则
func WithStreamComsumerAckMode(ack AckModeType) Option {
	return newFuncOption(func(o *Options) {
		o.AckMode = ack
	})
}

//WithStreamComsumerAckMode stream消费者专用,用于设定同步校验规则
func WithStreamComsumerStartAt(t time.Time) Option {
	return newFuncOption(func(o *Options) {
		n := t.UnixNano()
		ns := strconv.FormatInt(n, 10)
		o.Start = ns
	})
}

//WithStreamComsumerAckMode stream消费者专用,用于设定同步校验规则
func WithStreamComsumerStartAtID(id string) Option {
	return newFuncOption(func(o *Options) {
		o.Start = id
	})
}
