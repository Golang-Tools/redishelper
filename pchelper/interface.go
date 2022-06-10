// 生产者消费者模式(Producer-consumer problem)帮助工具.
package pchelper

import (
	"context"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
)

//AckModeType stream的Ack模式
type SerializeProtocolType uint8

const (

	//SerializeProtocol_JSON json作为序列化协议
	SerializeProtocol_JSON SerializeProtocolType = iota
	//SerializeProtocol_MSGPACK messagepack作为序列化协议
	SerializeProtocol_MSGPACK
)

//EventHanddler 处理消息的回调函数
//@params msg *Event Event对象
type EventHanddler func(msg *Event) error

//EventParser 用于将负载字符串转化为event的函数
//规定eventID不为""时解析流的消息,用到topic, eventID, payload
//规定eventID为""时解析除流之外的消息,用到SerializeProtocol,topic, payloadstr
type EventParser func(SerializeProtocol SerializeProtocolType, topic, eventID, payloadstr string, payload map[string]interface{}) (*Event, error)

//ConsumerInterface  消费者对象的接口
type ConsumerInterface interface {
	//RegistHandler 注册特定topic的执行函数
	//@params topic string 指定目标topic,`*`为全部topic
	//@params fn EventHanddler 事件触发时执行的函数
	RegistHandler(topic string, fn EventHanddler) error
	//UnRegistHandler 取消注册特定topic的执行函数
	//@params topic string 指定目标topic,`*`为全部topic
	UnRegistHandler(topic string) error
	//Listen 开始监听
	//@params topics string 指定监听的目标topic,使用`,`分隔表示多个topic
	//@params opts ...optparams.Option[ListenOptions] 监听时的一些配置,具体看listenoption.go说明
	Listen(topics string, opts ...optparams.Option[ListenOptions]) error
	//StopListening 停止监听
	StopListening() error
}

//ProducerInterface 生产者对象的接口
type ProducerInterface interface {
	//Publish 发布消息
	//@params ctx context.Context 发送的上下文配置
	//@params topic string 指定发送去的topic
	//@params payload interface{} 消息负载
	Publish(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[PublishOptions]) error
	//PubEvent 发布事件,事件会包含除负载外的一些其他元信息
	//@params ctx context.Context 发送的上下文配置
	//@params topic string 指定发送去的topic
	//@params payload interface{} 消息负载
	PubEvent(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[PublishOptions]) (*Event, error)
}

var logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "redis-producer-consumer-helper"}))
	logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}

//ProducerConsumerABC 消费者的基类
//定义了回调函数的注册操作和执行操作
type ProducerConsumerABC struct {
	Opt Options
}

func New(opts ...optparams.Option[Options]) *ProducerConsumerABC {
	l := new(ProducerConsumerABC)
	l.Opt = Defaultopt
	optparams.GetOption(&l.Opt, opts...)
	return l
}
