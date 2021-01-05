//Package queue 队列对象
package queue

import (
	"context"
	"fmt"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	message "github.com/Golang-Tools/message"
	helper "github.com/Golang-Tools/redishelper"
	"github.com/go-redis/redis/v8"
)

// Handdler 处理消息的回调函数
//@params msg Message 满足Message接口的对象
type Handdler func(msg message.Message) error

//Queue 消息队列
type Queue struct {
	MaxTTL          time.Duration
	client          helper.GoRedisV8Client
	handdlers       []Handdler
	handdlerslock            sync.Mutex
	listenCtxCancel context.CancelFunc
}

//NewQueue 新建一个PubSub主题
//@params client helper.GoRedisV8Client 客户端对象
//@params maxttl ...time.Duration 键的最长过期时间,最多填1位,不填则不设置
func NewQueue(client helper.GoRedisV8Client, maxttl ...time.Duration) *Queue {
	q := new(Queue)
	q.client = client
	q.handdlers = []Handdler{}
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

//RegistHanddler 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (q *Queue) RegistHanddler(fn Handdler) error {
	if q.listenCtxCancel != nil {
		return ErrQueueAlreadyListened
	}
	q.handdlerslock.Lock()
	q.handdlers = append(q.handdlers, fn)
	q.handdlerslock.Unlock()
	return nil
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 请求的上下文
//@params topics []string 要刷新的主题列表
func (q *Queue) RefreshTTL(ctx context.Context, topics ...string) error {
	if len(topics)<=0{
		return ErrNeedToPointOutTopics
	}
	if q.MaxTTL != 0 {
		_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range topics {
				_, err = pipe.Expire(ctx, key, q.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
		}
	}
	return ErrQueueNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 请求的上下文
//@params ctx context.Context 要查看的主题
func (q *Queue) TTL(ctx context.Context,topic string) (time.Duration, error) {
	res, err := q.client.TTL(ctx, q.Key).Result()
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
func (q *Queue) Len(ctx context.Context,topic string) (int64, error) {
	return q.client.LLen(ctx, q.Key).Result()
}

//Put 向队列中放入数据
//@params ctx context.Context 请求的上下文
func (q *Queue) Put(ctx context.Context, payload []byte,topics ... string) error {
	if len(topics)<=0{
		return ErrNeedToPointOutTopics
	}
	if refreshTTL {
		_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LPush(ctx, q.Key, msgbytes)
			pipe.Expire(ctx, q.Key, q.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err = q.client.LPush(ctx, q.Key, msgbytes).Result()
	if err != nil {
		return err
	}
	return nil
}

//Get 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
func (q *Queue) Get(ctx context.Context, refreshTTL bool, timeout time.Duration, fn FromBytesFunc) (Message, error) {
	if refreshTTL {
		defer q.RefreshTTL(ctx)
	}
	res, err := q.client.BRPop(ctx, timeout, q.Key).Result()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, fmt.Errorf("queue获得错误的返回: %v", res)
	}
	return fn([]byte(res[1]))
}

//GetNoWait 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
func (q *Queue) GetNoWait(ctx context.Context, refreshTTL bool, fn FromBytesFunc) (Message, error) {
	if refreshTTL {
		defer q.RefreshTTL(ctx)
	}
	res, err := q.client.RPop(ctx, q.Key).Result()
	if err != nil {
		return nil, err
	}
	return fn([]byte(res))
}

//Listen 监听一个队列
func (q *Queue) Listen(refreshTTL bool, fn FromBytesFunc, asyncHanddler bool) error {
	defer func() {
		q.listenCtxCancel = nil
	}()
	if q.listenCtxCancel != nil {
		return ErrQueueAlreadyListened
	}
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
				msg, err := q.Get(ctx, refreshTTL, 1*time.Second, fn)
				if err != nil {
					if err == redis.Nil {
						continue
					} else {
						log.Error("queue get message error", log.Dict{"err": err})
						return err
						// break Loop
					}

				} else {
					if asyncHanddler {
						for _, handdler := range q.handdlers {
							go func(handdler Handdler) {
								err := handdler(msg)
								if err != nil {
									log.Error("message handdler get error", log.Dict{"err": err})
								}
							}(handdler)
						}
					} else {
						for _, handdler := range q.handdlers {
							err := handdler(msg)
							if err != nil {
								log.Error("message handdler get error", log.Dict{"err": err})
							}
						}
					}
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
