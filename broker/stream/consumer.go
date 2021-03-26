//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package stream

import (
	"context"
	"sync"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/consumerabc"
	event "github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey/clientkeybatch"
	"github.com/go-redis/redis/v8"
)

//Consumer 流消费者对象
type Consumer struct {
	listenCtxCancel context.CancelFunc
	TopicInfos      []string
	RecvBatch       int64
	*clientkeybatch.ClientKeyBatch
	*consumerabc.ConsumerABC
	opt broker.Options
}

//NewConsumer 创建一个新的位图对象
//@params k *clientkeybatch.ClientKeyBatch redis客户端的批键对象
//@params start string 监听的起始位置
//@params recvBatch int64 一次获取的量
//@params opts ...broker.Option 生产者的配置
func NewConsumer(kb *clientkeybatch.ClientKeyBatch, start string, recvBatch int64, opts ...broker.Option) *Consumer {
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
	if recvBatch != 0 {
		c.RecvBatch = recvBatch
	} else {
		c.RecvBatch = 1
	}
	topics := []string{}
	starts := []string{}
	cstart := "$"
	if start != "" {
		cstart = start
	}
	for _, key := range c.Keys {
		topics = append(topics, key)
		starts = append(starts, cstart)
	}
	topics = append(topics, starts...)
	c.TopicInfos = topics
	return c
}

//Get 从多个流中取出数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间,为0则表示一直阻塞直到有数据
func (s *Consumer) Get(ctx context.Context, timeout time.Duration) ([]redis.XStream, error) {
	args := redis.XReadArgs{
		Streams: s.TopicInfos,
		Count:   s.RecvBatch,
		Block:   timeout,
	}
	return s.Client.XRead(ctx, &args).Result()
}

//Listen 监听一个流
//@params asyncHanddler bool 是否并行执行回调
//@params p ...Parser 监听队列的解析函数
func (s *Consumer) Listen(asyncHanddler bool, p ...event.Parser) error {
	if s.listenCtxCancel != nil {
		return ErrStreamConsumerAlreadyListened
	}
	defer func() {
		s.listenCtxCancel = nil
	}()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	s.listenCtxCancel = cancel
	// Loop:
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			{
				msgs, err := s.Get(ctx, 1*time.Second)
				if err != nil {
					switch err {
					case redis.Nil:
						{
							continue
						}
					case context.Canceled:
						{
							return nil
						}
					default:
						{
							log.Error("stream get message error", log.Dict{"err": err})
							return err
						}
					}
				} else {
					for _, xstream := range msgs {
						topic := xstream.Stream
						for _, xmsg := range xstream.Messages {
							eventID := xmsg.ID
							payload := xmsg.Values
							var evt *event.Event
							var err error
							switch len(p) {
							case 0:
								{
									evt, err = event.DefaultParser("", topic, eventID, "", payload)
								}
							default:
								{
									evt, err = p[0]("", topic, eventID, "", payload)
								}
							}
							if err != nil {
								log.Error("queue parser message error", log.Dict{"err": err})
								continue
							}
							s.ConsumerABC.HanddlerEvent(asyncHanddler, topic, evt)
						}
					}
				}
			}
		}
	}
}

//StopListening 停止监听
func (s *Consumer) StopListening() error {
	if s.listenCtxCancel == nil {
		return ErrStreamConsumerNotListeningYet
	}
	s.listenCtxCancel()
	return nil
}
