//Package 发布订阅器对象
package pubsubhelper

import (
	"context"
	"strings"

	"github.com/Golang-Tools/optparams"

	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"
)

//Consumer 发布订阅器消费者对象
type Consumer struct {
	cli          redis.UniversalClient
	listenPubsub *redis.PubSub
	*clientIdhelper.ClientIDAbc
	*pchelper.ConsumerABC
	opt Options
}

//NewConsumer 创建一个新的发布订阅器消费者对象
//@params cli redis.UniversalClient redis客户端对象
//@params opts ...optparams.Option[Options] 消费者的配置
func NewConsumer(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*Consumer, error) {
	c := new(Consumer)
	c.opt = defaultOptions
	optparams.GetOption(&c.opt, opts...)
	c.cli = cli
	c.ConsumerABC = pchelper.NewConsumerABC(c.opt.ProducerConsumerOpts...)
	meta, err := clientIdhelper.New(c.opt.ClientIDOpts...)
	if err != nil {
		return nil, err
	}
	c.ClientIDAbc = meta
	return c, nil
}

//Client 获取连接的redis客户端
func (s *Consumer) Client() redis.UniversalClient {
	return s.cli
}

//Listen 监听发布订阅器
//@params topics string 监听的topic,复数topic用`,`隔开
//@params opts ...optparams.Option[pchelper.ListenOptions] 监听时的一些配置,具体看listenoption.go说明
func (s *Consumer) Listen(topics string, opts ...optparams.Option[pchelper.ListenOptions]) error {
	if s.listenPubsub != nil {
		return ErrPubSubAlreadyListened
	}
	defer func() {
		s.listenPubsub = nil
	}()
	listenopt := pchelper.DefaultListenOpt
	optparams.GetOption(&listenopt, opts...)
	topic_slice := strings.Split(topics, ",")
	ctx := context.Background()
	pubsub := s.cli.Subscribe(ctx, topic_slice...)
	s.listenPubsub = pubsub
	ch := pubsub.Channel()
	for m := range ch {
		topic := m.Channel
		msg := m.Payload
		var evt *pchelper.Event
		var err error
		evt, err = listenopt.Parser(s.ConsumerABC.ProducerConsumerABC.Opt.SerializeProtocol, topic, "", msg, nil)

		if err != nil {
			logger.Error("consumer parser message error", map[string]any{"err": err})
			continue
		}
		s.ConsumerABC.HanddlerEvent(listenopt.ParallelHanddler, evt)
	}
	return nil
}

//StopListening 停止监听
func (s *Consumer) StopListening() error {
	if s.listenPubsub == nil {
		return ErrPubSubNotListeningYet
	}
	s.listenPubsub.Close()
	return nil
}
