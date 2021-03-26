//Package 发布订阅器对象
package pubsub

import (
	"context"
	"sync"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/consumerabc"
	event "github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey/clientkeybatch"
	"github.com/go-redis/redis/v8"
)

//Consumer 发布订阅器消费者对象
type Consumer struct {
	listenPubsub *redis.PubSub
	*clientkeybatch.ClientKeyBatch
	*consumerabc.ConsumerABC
	opt broker.Options
}

//NewConsumer 创建一个新的发布订阅器消费者对象
//@params k *clientkeybatch.ClientKeyBatch redis客户端的批键对象
//@params opts ...broker.Option 消费者的配置
func NewConsumer(kb *clientkeybatch.ClientKeyBatch, opts ...broker.Option) *Consumer {
	c := new(Consumer)
	c.ConsumerABC = &consumerabc.ConsumerABC{
		Handdlers:     map[string][]event.Handdler{},
		Handdlerslock: sync.RWMutex{},
	}
	c.ClientKeyBatch = kb
	c.opt = broker.Defaultopt
	for _, opt := range opts {
		opt.Apply(&c.opt)
	}
	return c
}

//Listen 监听发布订阅器
//@params asyncHanddler bool 是否并行执行回调
//@params p ...Parser 解析输入消息为事件对象的函数
func (s *Consumer) Listen(asyncHanddler bool, p ...event.Parser) error {
	if s.listenPubsub != nil {
		return ErrPubSubAlreadyListened
	}
	defer func() {
		s.listenPubsub = nil
	}()
	ctx := context.Background()
	pubsub := s.Client.Subscribe(ctx, s.Keys...)
	s.listenPubsub = pubsub
	ch := pubsub.Channel()
	for m := range ch {
		topic := m.Channel
		msg := m.Payload
		log.Debug("get msg", log.Dict{"msg": m})
		var evt *event.Event
		var err error
		switch len(p) {
		case 0:
			{
				evt, err = event.DefaultParser(s.opt.SerializeProtocol, topic, "", msg, nil)
			}
		default:
			{
				evt, err = p[0](s.opt.SerializeProtocol, topic, "", msg, nil)
			}
		}
		if err != nil {
			log.Error("pubsub parser message error", log.Dict{"err": err})
			continue
		}
		s.ConsumerABC.HanddlerEvent(asyncHanddler, evt)
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
