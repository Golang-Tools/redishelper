package streamhelper

import (
	"context"
	"strings"
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/clientIdhelper"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"
)

//Consumer 流消费者对象
type Consumer struct {
	cli             redis.UniversalClient
	listenCtxCancel context.CancelFunc
	opt             Options
	*clientIdhelper.ClientIDAbc
	*pchelper.ConsumerABC

	TopicInfos map[string]string
}

//NewConsumer 创建一个指向多个流的消费者
//默认一批获取一个消息,可以通过`WithStreamComsumerRecvBatchSize`设置批的大小
//如果使用`WithStreamComsumerGroupName`设定了group,则按组消费(默认总组监听的最新位置开始监听,收到消息后确认,消息确认策略可以通过`WithStreamComsumerAckMode`配置),
//否则按按单独客户端消费(默认从开始监听的时刻开始消费)
//需要注意单独客户端消费不会记录消费的偏移量,因此很容易丢失下次请求时的结果.解决方法是第一次使用`$`,后面则需要记录下id号从它开始
//@params cli redis.UniversalClient redis客户端对象
//@params opts ...optparams.Option[pchelper.Options] 消费者的配置
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
	c.cli = cli
	c.TopicInfos = map[string]string{}
	return c, nil
}

//Client 获取连接的redis客户端
func (s *Consumer) Client() redis.UniversalClient {
	return s.cli
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
		xstreams, err := s.cli.XRead(ctx, &args).Result()
		if len(xstreams) > 0 {
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
		Consumer: s.ClientID(),
		Streams:  topics,
		Count:    s.opt.RecvBatchSize,
		Block:    timeout,
	}
	if s.opt.AckMode == AckModeAckWhenGet {
		args.NoAck = false
	} else {
		args.NoAck = true
	}
	return s.cli.XReadGroup(ctx, &args).Result()
}

//Listen 监听流,默认情况下从所有topic的起始位置开始
//@params topics string 监听的topic,复数topic用`,`隔开
//@params opts ...optparams.Option[pchelper.ListenOptions] 监听时的一些配置,具体看listenoption.go说明
func (s *Consumer) Listen(topics string, opts ...optparams.Option[pchelper.ListenOptions]) error {
	if s.listenCtxCancel != nil {
		return ErrStreamConsumerAlreadyListened
	}
	defer func() {
		s.listenCtxCancel = nil
	}()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	s.listenCtxCancel = cancel
	listenopt := pchelper.DefaultListenOpt
	optparams.GetOption(&listenopt, opts...)
	topic_slice := strings.Split(topics, ",")
	for _, topic := range topic_slice {
		cstart, ok := listenopt.TopicStarts[topic]
		if ok {
			s.TopicInfos[topic] = cstart
		} else {
			if s.opt.DefaultStart != "" {
				s.TopicInfos[topic] = s.opt.DefaultStart
			} else {
				if s.opt.Group == "" {
					s.TopicInfos[topic] = "$"
				} else {
					s.TopicInfos[topic] = ">"
				}
			}
		}
	}
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
							logger.Error("stream consumer get message error", map[string]any{"err": err})
							return err
						}
					}
				} else {
					for _, xstream := range msgs {
						topic := xstream.Stream
						for _, xmsg := range xstream.Messages {
							eventID := xmsg.ID
							payload := xmsg.Values

							var evt *pchelper.Event
							var err error
							evt, err = listenopt.Parser(s.ProducerConsumerABC.Opt.SerializeProtocol, topic, eventID, "", payload)

							if err != nil {
								logger.Error("stream parser message error", map[string]any{"err": err})
								continue
							}
							s.ConsumerABC.HanddlerEvent(listenopt.ParallelHanddler, evt)
							if s.opt.Group != "" && s.opt.AckMode == AckModeAckWhenDone {
								_, err := s.cli.XAck(ctx, topic, s.opt.Group, eventID).Result()
								if err != nil {
									logger.Error("stream consumer ack get error",
										map[string]any{"err": err, "topic": topic, "group": s.opt.Group, "event_id": eventID, "client_id": s.ClientID()})
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
