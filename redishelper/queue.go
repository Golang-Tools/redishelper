package redishelper

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

//Queue 消息队列
type Queue struct {
	proxy *redisHelper
	Name  string
}

// QueueMessage 从队列中获取的消息
type QueueMessage struct {
	Topic   string
	Payload string
}

//NewQueue 新建一个PubSub主题
func NewQueue(proxy *redisHelper, name string) *Queue {
	s := new(Queue)
	s.Name = name
	s.proxy = proxy
	return s
}

// Len 查看当前队列长度
func (q *Queue) Len() (int64, error) {
	if !q.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := q.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.LLen(q.Name).Result()
}

//Put 向队列中放入数据
func (q *Queue) Put(values ...interface{}) (int64, error) {
	if !q.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := q.proxy.GetConn()
	if err != nil {
		return 0, err
	}

	res, err := conn.LPush(q.Name, values...).Result()
	if err != nil {
		fmt.Println("publish error:", err.Error())
	}
	return res, err
}

//Get 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
func (q *Queue) Get(timeout time.Duration) (*QueueMessage, error) {
	if !q.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := q.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	res, err := conn.BRPop(timeout, q.Name).Result()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, ErrQueueGetNotMatch
	}
	m := &QueueMessage{
		Topic:   res[0],
		Payload: res[1],
	}
	return m, nil
}

//GetNoWait 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
func (q *Queue) GetNoWait() (*QueueMessage, error) {
	if !q.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := q.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	res, err := conn.RPop(q.Name).Result()
	if err != nil {
		return nil, err
	}
	m := &QueueMessage{
		Topic:   q.Name,
		Payload: res,
	}
	return m, nil
}

//pubsubProducer 流对象
type queueProducer struct {
	Topic *Queue
}

func newQueueProducerFromQueue(topic *Queue) *queueProducer {
	s := new(queueProducer)
	s.Topic = topic
	return s
}

func newQueueProducer(proxy *redisHelper, topic string) *queueProducer {
	t := NewQueue(proxy, topic)
	s := newQueueProducerFromQueue(t)
	return s
}

//Publish 向流发送消息
func (producer *queueProducer) Publish(value interface{}) (int64, error) {
	return producer.Topic.Put(value)
}

type queueConsumer struct {
	stopch       chan error
	isSubscribed bool
	proxy        *redisHelper //使用的redis连接代理
	Topics       []string     //监听的topic
	pubsub       *redis.PubSub
}

func newQueueConsumer(proxy *redisHelper, topics []string) *queueConsumer {
	s := new(queueConsumer)
	s.stopch = make(chan error)
	s.isSubscribed = false
	s.proxy = proxy
	s.Topics = topics
	return s
}

func (consumer *queueConsumer) readOne(conn *redis.Client, timeout time.Duration) (*QueueMessage, error) {
	res, err := conn.BRPop(timeout, consumer.Topics...).Result()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, ErrQueueGetNotMatch
	}
	m := &QueueMessage{
		Topic:   res[0],
		Payload: res[1],
	}
	return m, nil
}
func (consumer *queueConsumer) Read(timeout time.Duration) (*QueueMessage, error) {
	if !consumer.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return consumer.readOne(conn, timeout)
}

//Read 订阅流,count可以
func (consumer *queueConsumer) Subscribe() (<-chan QueueMessage, error) {
	if !consumer.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	if consumer.isSubscribed {
		return nil, ErrIsAlreadySubscribed
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		fmt.Println("GetConn error: ", err.Error())
		return nil, err
	}
	ch := make(chan QueueMessage)
	go func() {
	Loop:
		for {
			select {
			case rec := <-consumer.stopch:
				{
					fmt.Println("exiting...")
					if rec == Done {
						close(ch)
						consumer.isSubscribed = false
						break Loop
					}
				}
			default:
				{
					res, err := consumer.readOne(conn, 1*time.Second)
					if err != nil {
						if err == redis.Nil {
							continue
						} else {
							fmt.Println("read one error: ", err.Error())
							close(ch)
							consumer.isSubscribed = false
							//panic(err)
							break Loop
						}
					} else {
						ch <- *res
					}
				}
			}
		}
	}()
	consumer.isSubscribed = true
	return ch, nil
}

//UnSubscribe 取消对特定频道的监听
func (consumer *queueConsumer) Close() error {
	if !consumer.isSubscribed {
		return ErrIsAlreadyUnSubscribed
	}
	consumer.stopch <- Done
	return nil
}
