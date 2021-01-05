package stream

//流消费者组
import (
	"context"
	"sync"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/go-redis/redis/v8"
)

//GroupConsumer 流消费者组对象
type GroupConsumer struct {
	MaxTTL          time.Duration
	client          redis.UniversalClient
	Group           string
	ConsumerName    string //监听使用的消费者名仅对消费组形式有效
	NoAck           bool   //是否不确认消息收到与否,仅对消费组形式有效
	handdlers       map[string][]Handdler
	handdlerslock   sync.RWMutex
	listenCtxCancel context.CancelFunc
}

//NewGroupConsumerOptions 创建流消费者组对象的可选参数
type NewGroupConsumerOptions struct {
	NoAck  bool
	MaxTTL time.Duration
}

//NewGroupConsumer 新建一个流对象的消费者
//@params client redis.UniversalClient 客户端对象
//@params key string 流使用的key
//@params option ...*NewGroupConsumerOptions 流的可选项
func NewGroupConsumer(client redis.UniversalClient, groupname, consumername string, option ...*NewGroupConsumerOptions) *GroupConsumer {
	s := new(GroupConsumer)
	s.client = client
	s.handdlers = map[string][]Handdler{}
	s.handdlerslock = sync.RWMutex{}
	s.Group = groupname
	s.ConsumerName = consumername
	switch len(option) {
	case 0:
		{
			return s
		}
	case 1:
		{
			op := option[0]
			if op != nil {
				if op.MaxTTL <= 0 {
					log.Warn("Maxttl不能小于等于0,设置无效")
				} else {
					s.MaxTTL = op.MaxTTL
				}
				s.NoAck = op.NoAck
				return s
			}
			log.Warn("option不能为nil,设置无效")
			return s
		}
	default:
		{
			log.Warn("option个数最多只能设置一个,使用第一个作为可选设置项")
			op := option[0]
			if op != nil {
				if op.MaxTTL <= 0 {
					log.Warn("Maxttl不能小于等于0,设置无效")
				} else {
					s.MaxTTL = op.MaxTTL
				}
				s.NoAck = op.NoAck
				return s
			}
			log.Warn("option不能为nil,设置无效")
			return s
		}
	}

}

//Subscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (s *GroupConsumer) Subscribe(topic string, fn Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	s.handdlerslock.Lock()
	_, ok := s.handdlers[topic]
	if ok {
		s.handdlers[topic] = append(s.handdlers[topic], fn)
	} else {
		s.handdlers[topic] = []Handdler{fn}
	}
	s.handdlerslock.Unlock()
	return nil
}

//UnSubscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (s *GroupConsumer) UnSubscribe(topic string) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	s.handdlerslock.Lock()
	_, ok := s.handdlers[topic]
	if ok {
		delete(s.handdlers, topic)
	}
	s.handdlerslock.Unlock()
	return nil
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 请求的上下文
//@params topics []string 要刷新的主题列表
func (s *GroupConsumer) RefreshTTL(ctx context.Context, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	if s.MaxTTL != 0 {
		_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range topics {
				pipe.Expire(ctx, key, s.MaxTTL)
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	return ErrConsumerNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 请求的上下文
func (s *GroupConsumer) TTL(ctx context.Context, topic string) (time.Duration, error) {
	res, err := s.client.TTL(ctx, topic).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, ErrStreamNotExist
		}
		return 0, err
	}
	return res, nil
}

//Get 从多个队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间
//@params topics ...string 队列列表
func (s *GroupConsumer) Get(ctx context.Context, timeout time.Duration, count int64, topicinfos ...*TopicInfo) ([]redis.XStream, error) {
	if len(topicinfos) <= 0 {
		return nil, ErrNeedToPointOutTopics
	}
	topics := []string{}
	starts := []string{}
	for _, i := range topicinfos {
		if i.Topic != "" {
			start := i.Start
			if i.Start == "" {
				start = "$"
			}
			topics = append(topics, i.Topic)
			starts = append(starts, start)
		}
	}
	topics = append(topics, starts...)
	if s.MaxTTL != 0 {
		defer s.RefreshTTL(ctx, topics...)
	}
	args := redis.XReadGroupArgs{
		Group:    s.Group,
		Consumer: s.ConsumerName,
		Streams:  topics,
		Count:    count,
		Block:    timeout,
		NoAck:    s.NoAck,
	}
	return s.client.XReadGroup(ctx, &args).Result()
}

//GetNoWait 从一个队列中尝试取出数据
//@params ctx context.Context 请求的上下文
//@params topic string 队列名
func (s *GroupConsumer) GetNoWait(ctx context.Context, count int64, topicinfos ...*TopicInfo) ([]redis.XStream, error) {
	if len(topicinfos) <= 0 {
		return nil, ErrNeedToPointOutTopics
	}
	topics := []string{}
	starts := []string{}
	for _, i := range topicinfos {
		if i.Topic != "" {
			start := i.Start
			if i.Start == "" {
				start = "$"
			}
			topics = append(topics, i.Topic)
			starts = append(starts, start)
		}
	}
	topics = append(topics, starts...)
	if s.MaxTTL != 0 {
		defer s.RefreshTTL(ctx, topics...)
	}
	args := redis.XReadGroupArgs{
		Group:    s.Group,
		Consumer: s.ConsumerName,
		Streams:  topics,
		Count:    count,
		NoAck:    s.NoAck,
	}
	return s.client.XReadGroup(ctx, &args).Result()
}

//Listen 监听一个流
func (s *GroupConsumer) Listen(asyncHanddler bool, topicinfos ...*TopicInfo) error {
	if s.listenCtxCancel != nil {
		return ErrConsumerAlreadyListened
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
				msg, err := s.Get(ctx, 1*time.Second, 1, topicinfos...)
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
					s.handdlerslock.Lock()
					streamMessage := msg[0]
					handdlers, ok := s.handdlers[streamMessage.Stream]
					if ok {
						if asyncHanddler {
							for _, handdler := range handdlers {
								go func(handdler Handdler) {
									err := handdler(&streamMessage)
									if err != nil {
										log.Error("message handdler get error", log.Dict{"err": err})
									}
								}(handdler)
							}
						} else {
							for _, handdler := range handdlers {
								err := handdler(&streamMessage)
								if err != nil {
									log.Error("message handdler get error", log.Dict{"err": err})
								}
							}
						}
					}
					s.handdlerslock.Unlock()
				}
			}
		}
	}
}

//StopListening 停止监听
func (s *GroupConsumer) StopListening() error {
	if s.listenCtxCancel == nil {
		return ErrConsumerNotListeningYet
	}
	s.listenCtxCancel()
	return nil
}
