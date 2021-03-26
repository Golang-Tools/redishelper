//Package stream 流及相关对象的包
package stream

import (
	"context"
	"strconv"
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
	TopicInfos      map[string]string
	*clientkeybatch.ClientKeyBatch
	*consumerabc.ConsumerABC
	opt broker.Options
}

//NewConsumer 创建一个指向多个流的消费者
//默认一批获取一个消息,可以通过`WithStreamComsumerRecvBatchSize`设置批的大小
//如果使用`WithStreamComsumerGroupName`设定了group,则按组消费(默认总组监听的最新位置开始监听,收到消息后确认,消息确认策略可以通过`WithStreamComsumerAckMode`配置),
//否则按按单独客户端消费(默认从开始监听的时刻开始消费)
//需要注意单独客户端消费不会记录消费的偏移量,因此很容易丢失下次请求时的结果.解决方法是第一次使用`$`,后面则需要记录下id号从它开始
//@params k *clientkeybatch.ClientKeyBatch redis客户端的批键对象
//@params start string 监听的起始位置,可以是一个unix毫秒时间戳的字符串
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
	var cstart string
	if c.opt.Group == "" {
		cstart = "$"
	} else {
		cstart = ">"
	}
	if c.opt.Start != "" {
		cstart = c.opt.Start
	}
	c.TopicInfos = map[string]string{}
	for _, key := range c.Keys {
		c.TopicInfos[key] = cstart
	}
	return c
}

//Get 从多个流中取出数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间,为0则表示一直阻塞直到有数据
func (s *Consumer) Get(ctx context.Context, timeout time.Duration) ([]redis.XStream, error) {
	topics := []string{}
	starts := []string{}
	for topic, start := range s.TopicInfos {
		topics = append(topics, topic)
		starts = append(starts, start)
	}
	topics = append(topics, starts...)
	if s.opt.Group == "" {
		args := redis.XReadArgs{
			Streams: topics,
			Count:   s.opt.RecvBatchSize,
			Block:   timeout,
		}
		xstreams, err := s.Client.XRead(ctx, &args).Result()
		if xstreams != nil {
			for _, xstream := range xstreams {
				tt := xstream.Stream
				latest := xstream.Messages[len(xstream.Messages)-1]
				s.TopicInfos[tt] = latest.ID
			}
		}
		return xstreams, err
	}
	args := redis.XReadGroupArgs{
		Group:    s.opt.Group,
		Consumer: strconv.FormatUint(uint64(s.opt.ClientID), 16),
		Streams:  topics,
		Count:    s.opt.RecvBatchSize,
		Block:    timeout,
	}
	if s.opt.AckMode == broker.AckModeAckWhenGet {
		args.NoAck = false
	} else {
		args.NoAck = true
	}
	return s.Client.XReadGroup(ctx, &args).Result()
}

//Listen 监听流
//@params asyncHanddler bool 是否并行执行回调
//@params p ...Parser 解析输入消息为事件对象的函数
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
				msgs, err := s.Get(ctx, s.opt.BlockTime)
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
							log.Error("sstream consumer get message error", log.Dict{"err": err})
							return err
						}
					}
				} else {
					// log.Info("stream consumer get message", log.Dict{"err": err, "msgs": msgs, "group": s.opt.Group, "TopicInfos": s.TopicInfos})
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
							// log.Info("stream consumer get event", log.Dict{"err": err, "event": evt})
							if err != nil {
								log.Error("stream consumer parser message error", log.Dict{"err": err})
								continue
							}
							s.ConsumerABC.HanddlerEvent(asyncHanddler, evt)
							if s.opt.Group != "" && s.opt.AckMode == broker.AckModeAckWhenDone {
								_, err := s.Client.XAck(ctx, topic, s.opt.Group, eventID).Result()
								if err != nil {
									log.Error("stream consumer ack get error",
										log.Dict{"err": err, "topic": topic, "group": s.opt.Group, "event_id": eventID, "client_id": s.opt.ClientID})
									continue
								}
							}
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
