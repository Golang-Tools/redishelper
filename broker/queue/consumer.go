//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"sync"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/v2/broker"
	"github.com/Golang-Tools/redishelper/v2/broker/consumerabc"
	event "github.com/Golang-Tools/redishelper/v2/broker/event"
	"github.com/Golang-Tools/redishelper/v2/clientkey/clientkeybatch"
	"github.com/go-redis/redis/v8"
)

//Consumer 队列消费者对象
type Consumer struct {
	listenCtxCancel context.CancelFunc
	*clientkeybatch.ClientKeyBatch
	*consumerabc.ConsumerABC
	opt broker.Options
}

//NewConsumer 创建一个新的队列消费者对象
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
func (s *Consumer) Listen(asyncHanddler bool, p ...event.Parser) error {
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
				topic, msg, err := s.Get(ctx, s.opt.BlockTime)
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
							evt, err = event.DefaultParser(s.opt.SerializeProtocol, topic, "", msg, nil)
						}
					default:
						{
							evt, err = p[0](s.opt.SerializeProtocol, topic, "", msg, nil)
						}
					}
					if err != nil {
						log.Error("queue parser message error", log.Dict{"err": err})
						continue
					}
					s.ConsumerABC.HanddlerEvent(asyncHanddler, evt)
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

//AsQueueArray 从消费者构造由队列对象组成的序列
//序列顺序与keys中键的顺序一致
func (s *Consumer) AsQueueArray() []*Queue {
	l := s.ClientKeyBatch.ToArray()
	result := []*Queue{}
	for _, k := range l {
		q := New(k)
		result = append(result, q)
	}
	return result
}
