//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queuehelper

import (
	"context"
	"strings"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"
)

//Consumer 队列消费者对象
type Consumer struct {
	cli             redis.UniversalClient
	listenCtxCancel context.CancelFunc
	opt             Options
	*clientIdhelper.ClientIDAbc
	*pchelper.ConsumerABC
}

//NewConsumer 创建一个新的队列消费者对象
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

//Get 从多个队列中取出数据,timeout为0则表示一直阻塞直到有数据
//@params ctx context.Context 请求的上下文
//@params timeout time.Duration 等待超时时间
//@params topics ...string 获取的指定队列
//@returns string, string, error 依顺序为topic,payload,err
func (s *Consumer) Get(ctx context.Context, timeout time.Duration, topics ...string) (string, string, error) {
	res, err := s.cli.BRPop(ctx, timeout, topics...).Result()
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
//@params topics string 监听的topic,复数topic用`,`隔开
//@params opts ...optparams.Option[pchelper.ListenOptions] 监听时的一些配置,具体看listenoption.go说明
func (s *Consumer) Listen(topics string, opts ...optparams.Option[pchelper.ListenOptions]) error {
	if s.listenCtxCancel != nil {
		return ErrQueueAlreadyListened
	}
	defer func() {
		s.listenCtxCancel = nil
	}()
	listenopt := pchelper.DefaultListenOpt
	optparams.GetOption(&listenopt, opts...)
	topic_slice := strings.Split(topics, ",")
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
				topic, msg, err := s.Get(ctx, s.opt.BlockTime, topic_slice...)
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
							logger.Error("queue get message error", map[string]any{"err": err})
							return err
						}
					}
				} else {
					var evt *pchelper.Event
					var err error
					evt, err = listenopt.Parser(s.ConsumerABC.ProducerConsumerABC.Opt.SerializeProtocol, topic, "", msg, nil)

					if err != nil {
						logger.Error("queue parser message error", map[string]any{"err": err})
						continue
					}
					s.ConsumerABC.HanddlerEvent(listenopt.ParallelHanddler, evt)
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

// Len 查看当前队列长度
//@params ctx context.Context 请求的上下文
//@params topic string 指定要查看的队列名
func (s *Consumer) Len(ctx context.Context, topic string) (int64, error) {
	return s.cli.LLen(ctx, topic).Result()
}
