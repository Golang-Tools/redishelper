package pubsub

import (
	"context"
	"sync"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/message"
	"github.com/go-redis/redis/v8"
)

//PubSub 发布订阅器
type PubSub struct {
	client        redis.UniversalClient
	handdlers     map[string][]message.Handdler
	handdlerslock sync.RWMutex
	listenPubsub  *redis.PubSub
}

//New 新建一个发布订阅器
func New(client redis.UniversalClient) *PubSub {
	s := new(PubSub)
	s.client = client
	s.handdlers = map[string][]message.Handdler{}
	s.handdlerslock = sync.RWMutex{}
	return s
}

//Subscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (p *PubSub) Subscribe(topic string, fn message.Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	p.handdlerslock.Lock()
	_, ok := p.handdlers[topic]
	if ok {
		p.handdlers[topic] = append(p.handdlers[topic], fn)
	} else {
		p.handdlers[topic] = []message.Handdler{fn}
	}
	p.handdlerslock.Unlock()
	return nil
}

//UnSubscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (p *PubSub) UnSubscribe(topic string) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	p.handdlerslock.Lock()
	_, ok := p.handdlers[topic]
	if ok {
		delete(p.handdlers, topic)
	}
	p.handdlerslock.Unlock()
	return nil
}

//Publish 向发布订阅器中放入数据
//@params ctx context.Context 请求的上下文
//@params payload []byte 发送的消息负载
//@params topics ...string 发送消息到哪些key
func (p *PubSub) Publish(ctx context.Context, payload []byte, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	_, err := p.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range topics {
			pipe.Publish(ctx, key, payload)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//Listen 监听一个发布订阅器
//@params asyncHanddler bool 是否并行执行回调
//@params topics ...string 监听的发布订阅器
func (p *PubSub) Listen(asyncHanddler bool, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	if p.listenPubsub != nil {
		return ErrPubSubAlreadyListened
	}
	defer func() {
		p.listenPubsub = nil
	}()
	ctx := context.Background()
	pubsub := p.client.Subscribe(ctx, topics...)
	p.listenPubsub = pubsub
	ch := pubsub.Channel()
	for m := range ch {
		msg, _ := message.NewFromRedisMessage(m)
		p.handdlerslock.Lock()
		handdlers, ok := p.handdlers[msg.Topic]
		if ok {
			if asyncHanddler {
				for _, handdler := range handdlers {
					go func(handdler message.Handdler) {
						err := handdler(msg)
						if err != nil {
							log.Error("message handdler get error", log.Dict{"err": err})
						}
					}(handdler)
				}
			} else {
				for _, handdler := range handdlers {
					err := handdler(msg)
					if err != nil {
						log.Error("message handdler get error", log.Dict{"err": err})
					}
				}
			}
		}
		p.handdlerslock.Unlock()
	}
	return nil
}

//StopListening 停止监听
func (p *PubSub) StopListening() error {
	if p.listenPubsub == nil {
		return ErrPubSubNotListeningYet
	}
	p.listenPubsub.Close()
	return nil
}
