package streamhelper

import (
	"context"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	redis "github.com/go-redis/redis/v8"
)

//Producer 流的生产者对象
type Producer struct {
	cli redis.UniversalClient
	opt Options
	*pchelper.ProducerConsumerABC
	*clientIdhelper.ClientIDAbc
}

//NewProducer 创建一个新的流生产者
//@params k *clientkey.ClientKey redis客户端的键对象
//@params opts ...broker.Option 生产者的配置
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

//Client 获取连接的redis客户端
func (p *Producer) Client() redis.UniversalClient {
	return p.cli
}

//Publish 向流中放入数据
//@params ctx context.Context 请求的上下文
//@params payload interface{} 发送的消息负载,负载如果不是map[string]interface{}形式或者可以被json/msgpack序列化的对象则统一以[value 值]的形式传出
func (p *Producer) Publish(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[pchelper.PublishOptions]) error {
	args := redis.XAddArgs{}
	Values, err := pchelper.ToXAddArgsValue(p.ProducerConsumerABC.Opt.SerializeProtocol, payload)
	if err != nil {
		return err
	}
	args.Values = Values

	args.Stream = topic
	opt := pchelper.DefaultPublishOpt
	optparams.GetOption(&opt, opts...)
	if opt.ID != "" {
		args.ID = opt.ID
	} else {
		args.ID = "*"
	}
	if opt.Limit > 0 {
		args.Limit = opt.Limit
	}
	if opt.MinID != "" {
		args.MinID = opt.MinID
	}
	if opt.NoMkStream {
		args.NoMkStream = true
	}
	if opt.MaxLen > 0 {
		args.MaxLen = opt.MaxLen
	} else {
		if p.opt.DefaultMaxLen > 0 {
			args.MaxLen = p.opt.DefaultMaxLen
		}
	}
	if opt.Strict {
		args.Approx = true
	} else {
		if p.opt.DefaultStrict {
			args.Approx = true
		}
	}
	_, err = p.cli.XAdd(ctx, &args).Result()
	return err
}

//PubEvent 向流中放入事件数据
//@params ctx context.Context 请求的上下文
//@params payload []byte 发送的消息负载
//@returns *event.Event 发送出去的消息对象
func (p *Producer) PubEvent(ctx context.Context, topic string, payload interface{}, opts ...optparams.Option[pchelper.PublishOptions]) (*pchelper.Event, error) {
	msg := pchelper.Event{
		EventTime: time.Now().UnixNano(),
		Payload:   payload,
		Topic:     topic,
	}
	if p.ClientID() != "" {
		msg.Sender = p.ClientID()
	}
	err := p.Publish(ctx, topic, msg, opts...)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
