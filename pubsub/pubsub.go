package redishelper

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v7"
)

//PubSubTopic 流主题
type PubSubTopic struct {
	proxy  *redisHelper
	Name   string
	pubsub *redis.PubSub
}

//NewPubSubTopic 新建一个PubSub主题
func NewPubSubTopic(proxy *redisHelper, name string) *PubSubTopic {
	s := new(PubSubTopic)
	s.Name = name
	s.proxy = proxy
	return s
}

//Publish 向发布订阅主题发送消息
func (topic *PubSubTopic) Publish(value interface{}) (int64, error) {
	if !topic.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return 0, err
	}

	res, err := conn.Publish(topic.Name, value).Result()
	if err != nil {
		fmt.Println("publish error:", err.Error())
	}
	return res, err
}

func (topic *PubSubTopic) Subscribe(ctx context.Context) (<-chan *redis.Message, error) {
	if !topic.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return nil, err
	}

	topic.pubsub = conn.WithContext(ctx).Subscribe(topic.Name)
	_, err = topic.pubsub.Receive()
	if err != nil {
		return nil, err
	}
	ch := topic.pubsub.Channel()
	return ch, err
}

//UnSubscribe 取消订阅
func (topic *PubSubTopic) UnSubscribe() error {
	if topic.pubsub == nil {
		return ErrPubSubNotSubscribe
	}
	return topic.pubsub.Unsubscribe(topic.Name)
}

//Close 关闭监听
func (topic *PubSubTopic) Close() error {
	if topic.pubsub == nil {
		return ErrPubSubNotSubscribe
	}
	return topic.pubsub.Close()
}

//pubsubProducer 流对象
type pubsubProducer struct {
	Topic *PubSubTopic
}

func newPubSubProducerFromPubSubTopic(topic *PubSubTopic) *pubsubProducer {
	s := new(pubsubProducer)
	s.Topic = topic
	return s
}

func newPubSubProducer(proxy *redisHelper, topic string) *pubsubProducer {
	t := NewPubSubTopic(proxy, topic)
	s := newPubSubProducerFromPubSubTopic(t)
	return s
}

//Publish 向流发送消息
func (producer *pubsubProducer) Publish(value interface{}) (int64, error) {
	return producer.Topic.Publish(value)
}

type pubsubConsumer struct {
	proxy  *redisHelper //使用的redis连接代理
	Topics []string     //监听的topic
	pubsub *redis.PubSub
}

func newPubSubConsumer(proxy *redisHelper, topics []string) *pubsubConsumer {
	s := new(pubsubConsumer)
	s.proxy = proxy
	s.Topics = topics
	return s
}

//Read 订阅流,count可以
func (consumer *pubsubConsumer) Subscribe(ctx context.Context) (<-chan *redis.Message, error) {
	if !consumer.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	consumer.pubsub = conn.WithContext(ctx).Subscribe(consumer.Topics...)
	_, err = consumer.pubsub.Receive()
	if err != nil {
		return nil, err
	}
	ch := consumer.pubsub.Channel()
	return ch, nil
}

//UnSubscribe 取消对特定频道的监听
func (consumer *pubsubConsumer) UnSubscribe(topics ...string) error {
	if consumer.pubsub == nil {
		return ErrPubSubNotSubscribe
	}
	return consumer.pubsub.Unsubscribe(topics...)
}

//Close 关闭监听
func (consumer *pubsubConsumer) Close() error {
	if consumer.pubsub == nil {
		return ErrPubSubNotSubscribe
	}
	return consumer.pubsub.Close()
}
