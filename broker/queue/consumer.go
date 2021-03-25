//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"sync"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker"
	event "github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey/clientkeybatch"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
)

type Parser func(topic, payload string) (*event.Event, error)

//Consumer 流消费者对象
type Consumer struct {
	handdlers       map[string][]event.Handdler
	handdlerslock   sync.RWMutex
	listenCtxCancel context.CancelFunc
	*clientkeybatch.ClientKeyBatch
	opt broker.Options
}

//NewConsumer 创建一个新的位图对象
//@params k *clientkeybatch.ClientKeyBatch redis客户端的批键对象
//@params opts ...broker.Option 生产者的配置
func NewConsumer(kb *clientkeybatch.ClientKeyBatch, opts ...broker.Option) *Consumer {
	c := new(Consumer)
	c.handdlers = map[string][]event.Handdler{}
	c.handdlerslock = sync.RWMutex{}
	c.ClientKeyBatch = kb
	defaultopt := broker.Options{
		SerializeProtocol: "JSON",
		ClientID:          randomkey.GetMachineID(),
		UUIDType:          "sonyflake",
	}
	c.opt = defaultopt
	for _, opt := range opts {
		opt.Apply(&c.opt)
	}
	return c
}

//RegistHandler 将回调函数注册到queue上
//@params topic string 注册的topic
//@params fn event.Handdler 注册到topic上的回调函数
func (c *Consumer) RegistHandler(topic string, fn event.Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	c.handdlerslock.Lock()
	_, ok := c.handdlers[topic]
	if ok {
		c.handdlers[topic] = append(c.handdlers[topic], fn)
	} else {
		c.handdlers[topic] = []event.Handdler{fn}
	}
	c.handdlerslock.Unlock()
	return nil
}

//UnSubscribe 将回调函数注册到queue上
//@params fn Handdler 注册到消息上的回调函数
func (s *Consumer) UnSubscribe(topic string) error {
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

func (s *Consumer) defaultParser(topic, payload string) (*event.Event, error) {
	m := event.Event{}
	m.Topic = topic
	switch s.opt.SerializeProtocol {
	case "JSON":
		{
			err := json.Unmarshal([]byte(payload), &m)
			if err != nil {
				if err != nil || m.EventID == "" {
					// log.Error("default parser message error 1", log.Dict{"err": err})
					p := map[string]interface{}{}
					err := json.Unmarshal([]byte(payload), &p)
					if err != nil {
						// log.Error("default parser message error 2", log.Dict{"err": err})
						m.Payload = string(payload)
					} else {
						m.Payload = p
					}
				}
			}
			return &m, nil
		}
	case "msgpack":
		{
			err := msgpack.Unmarshal([]byte(payload), &m)
			if err != nil || m.EventID == "" {
				p := map[string]interface{}{}
				err := json.Unmarshal([]byte(payload), &p)
				if err != nil {
					// log.Error("default parser message error 2", log.Dict{"err": err})
					m.Payload = string(payload)
				} else {
					m.Payload = p
				}
			}
			return &m, nil
		}
	default:
		{
			return nil, broker.ErrUnSupportSerializeProtocol
		}
	}
}

//Get 从多个队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间
//@returns string, string, error 依顺序为topic,payload,err
func (s *Consumer) Get(ctx context.Context, timeout time.Duration) (string, string, error) {
	if s.Opt.MaxTTL != 0 {
		defer s.RefreshTTL(ctx)
	}
	res, err := s.Client.BRPop(ctx, timeout, s.Keys...).Result()
	if err != nil {
		return "", "", err
	}
	if len(res) != 2 {
		return "", "", ErrQueueResNotTwo
	}
	topic := res[0]
	payload := res[1]
	return topic, payload, nil
}

//Listen 监听一个队列
//@params asyncHanddler bool 是否并行执行回调
//@params p ...Parser 监听队列的解析函数
func (s *Consumer) Listen(asyncHanddler bool, p ...Parser) error {
	if s.listenCtxCancel != nil {
		return ErrQueueAlreadyListened
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
				topic, msg, err := s.Get(ctx, 1*time.Second)
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
					var evt *event.Event
					var err error
					switch len(p) {
					case 0:
						{
							evt, err = s.defaultParser(topic, msg)
						}
					default:
						{
							evt, err = p[0](topic, msg)
						}
					}
					if err != nil {
						log.Error("queue parser message error", log.Dict{"err": err})
						continue
					}
					s.handdlerslock.Lock()
					handdlers, ok := s.handdlers[topic]
					if ok {
						if asyncHanddler {
							for _, handdler := range handdlers {
								go func(handdler event.Handdler) {
									err := handdler(evt)
									if err != nil {
										log.Error("message handdler get error", log.Dict{"err": err})
									}
								}(handdler)
							}
						} else {
							for _, handdler := range handdlers {
								err := handdler(evt)
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
func (s *Consumer) StopListening() error {
	if s.listenCtxCancel == nil {
		return ErrQueueNotListeningYet
	}
	s.listenCtxCancel()
	return nil
}

func (s *Consumer) AsQueueArray() []*Queue {
	l := s.ClientKeyBatch.ToArray()
	result := []*Queue{}
	for _, k := range l {
		q := New(k)
		result = append(result, q)
	}
	return result
}
