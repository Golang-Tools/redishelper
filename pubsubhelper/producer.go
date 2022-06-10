//Package pubsub 发布订阅器对象
//非常适合作为简单的广播模式的中间件
package pubsubhelper

import (
	"context"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"

	"github.com/Golang-Tools/idgener"
)

//Producer 发布订阅器生产者对象
type Producer struct {
	cli redis.UniversalClient
	*pchelper.ProducerConsumerABC
	*clientIdhelper.ClientIDAbc
	opt Options
}

//NewProducer 创建一个新的发布订阅器的生产者
//@params cli redis.UniversalClient redis客户端对象
//@params opts ...optparams.Option[Options] 生产者的配置
func NewProducer(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Producer, error) {
	c := new(Producer)
	c.cli = cli
	c.opt = defaultOptions
	optparams.GetOption(&c.opt, opts...)
	meta, err := clientIdhelper.New(c.opt.ClientIDOpts...)
	if err != nil {
		return nil, err
	}
	c.ClientIDAbc = meta
	pc := pchelper.New(c.opt.ProducerConsumerOpts...)
	c.ProducerConsumerABC = pc
	return c, nil
}

//Publish 向队列中放入数据
//@params ctx context.Context 请求的上下文
//@params topic string 发送去的指定频道
//@params payload interface{} 发送的消息负载,负载支持string,bytes,bool,number,以及可以被json或者msgpack序列化的对象
//@params opts ...optparams.Option[pchelper.PublishOptions] 无效
func (p *Producer) Publish(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[pchelper.PublishOptions]) error {
	payloadbytes, err := pchelper.ToBytes(p.ProducerConsumerABC.Opt.SerializeProtocol, payload)
	if err != nil {
		return err
	}
	_, err = p.cli.Publish(ctx, topic, payloadbytes).Result()
	return err
}

//PubEvent 向发布订阅器中放入事件数据
//@params ctx context.Context 请求的上下文
//@params topic string 发送去的指定频道
//@params payload []byte 发送的消息负载
//@params opts ...optparams.Option[pchelper.PublishOptions] 无效
//@returns *pchelper.Event 发送出去的消息对象
func (p *Producer) PubEvent(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[pchelper.PublishOptions]) (*pchelper.Event, error) {
	msg := pchelper.Event{
		EventTime: time.Now().UnixNano(),
		Payload:   payload,
		Topic:     topic,
	}
	if p.ClientID() != "" {
		msg.Sender = p.ClientID()
	}
	mid, err := idgener.Next(p.ProducerConsumerABC.Opt.UUIDType)
	if err != nil {
		return nil, err
	}
	msg.EventID = mid
	err = p.Publish(ctx, topic, msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
