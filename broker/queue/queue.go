//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"sync"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	message "github.com/Golang-Tools/redishelper/broker/message"
	"github.com/go-redis/redis/v8"
)

//Queue 消息队列
type Queue struct {
	MaxTTL          time.Duration
	client          redis.UniversalClient
	handdlers       map[string][]message.Handdler
	handdlerslock   sync.RWMutex
	listenCtxCancel context.CancelFunc
}

//New 新建一个PubSub主题
//@params client redis.UniversalClient 客户端对象
//@params maxttl ...time.Duration 键的最长过期时间,最多填1位,不填则不设置
func New(client redis.UniversalClient, maxttl ...time.Duration) *Queue {
	q := new(Queue)
	q.client = client
	q.handdlers = map[string][]message.Handdler{}
	q.handdlerslock = sync.RWMutex{}
	switch len(maxttl) {
	case 0:
		{
			return q
		}
	case 1:
		{
			if maxttl[0] != 0 {
				q.MaxTTL = maxttl[0]
				return q
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return q
		}
	default:
		{
			log.Warn("ttl最多只能设置一个,使用第一个作为过期时间")
			if maxttl[0] != 0 {
				q.MaxTTL = maxttl[0]
				return q
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return q
		}
	}
}

//Subscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (q *Queue) Subscribe(topic string, fn message.Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	q.handdlerslock.Lock()
	_, ok := q.handdlers[topic]
	if ok {
		q.handdlers[topic] = append(q.handdlers[topic], fn)
	} else {
		q.handdlers[topic] = []message.Handdler{fn}
	}
	q.handdlerslock.Unlock()
	return nil
}

//UnSubscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (q *Queue) UnSubscribe(topic string) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	q.handdlerslock.Lock()
	_, ok := q.handdlers[topic]
	if ok {
		delete(q.handdlers, topic)
	}
	q.handdlerslock.Unlock()
	return nil
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 请求的上下文
//@params topics []string 要刷新的主题列表
func (q *Queue) RefreshTTL(ctx context.Context, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	if q.MaxTTL != 0 {
		_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range topics {
				pipe.Expire(ctx, key, q.MaxTTL)
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	return ErrQueueNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 请求的上下文
//@params ctx context.Context 要查看的主题
func (q *Queue) TTL(ctx context.Context, topic string) (time.Duration, error) {
	res, err := q.client.TTL(ctx, topic).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, ErrTopicNotExist
		}
		return 0, err
	}
	return res, nil
}

// Len 查看当前队列长度
//@params ctx context.Context 请求的上下文
//@params ctx context.Context 要查看的主题
func (q *Queue) Len(ctx context.Context, topic string) (int64, error) {
	return q.client.LLen(ctx, topic).Result()
}

//Publish 向队列中放入数据
//@params ctx context.Context 请求的上下文
//@params payload []byte 发送的消息负载
//@params topics ...string 发送消息到哪些key
func (q *Queue) Publish(ctx context.Context, payload []byte, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range topics {
			pipe.LPush(ctx, key, payload)
			if q.MaxTTL != 0 {
				pipe.Expire(ctx, key, q.MaxTTL)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//Get 从多个队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间
//@params topics ...string 队列列表
func (q *Queue) Get(ctx context.Context, timeout time.Duration, topics ...string) (*message.Message, error) {
	if len(topics) <= 0 {
		return nil, ErrNeedToPointOutTopics
	}
	if q.MaxTTL != 0 {
		defer q.RefreshTTL(ctx, topics...)
	}
	res, err := q.client.BRPop(ctx, timeout, topics...).Result()
	if err != nil {
		return nil, err
	}
	return message.NewFromQueue(res)
}

//GetNoWait 从一个队列中尝试取出数据
//@params ctx context.Context 请求的上下文
//@params topic string 队列名
func (q *Queue) GetNoWait(ctx context.Context, topic string) (*message.Message, error) {
	if q.MaxTTL != 0 {
		defer q.RefreshTTL(ctx, topic)
	}
	res, err := q.client.RPop(ctx, topic).Result()
	if err != nil {
		return nil, err
	}
	m := message.Message{
		Topic:   topic,
		Payload: []byte(res),
	}
	return &m, nil
}

//Listen 监听一个队列
//@params asyncHanddler bool 是否并行执行回调
//@params topics ...string 监听的队列
func (q *Queue) Listen(asyncHanddler bool, topics ...string) error {
	if len(topics) <= 0 {
		return ErrNeedToPointOutTopics
	}
	if q.listenCtxCancel != nil {
		return ErrQueueAlreadyListened
	}
	defer func() {
		q.listenCtxCancel = nil
	}()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	q.listenCtxCancel = cancel
	// Loop:
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			{
				msg, err := q.Get(ctx, 1*time.Second, topics...)
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
							log.Error("queue get message error", log.Dict{"err": err})
							return err
						}
					}
				} else {
					q.handdlerslock.Lock()
					handdlers, ok := q.handdlers[msg.Topic]
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
					q.handdlerslock.Unlock()
				}
			}
		}
	}
}

//StopListening 停止监听
func (q *Queue) StopListening() error {
	if q.listenCtxCancel == nil {
		return ErrQueueNotListeningYet
	}
	q.listenCtxCancel()
	return nil
}
