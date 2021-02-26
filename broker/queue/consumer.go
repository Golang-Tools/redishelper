//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"sync"

	message "github.com/Golang-Tools/redishelper/broker/message"
	"github.com/Golang-Tools/redishelper/clientkey"
)

//Consumer 流消费者对象
type Consumer struct {
	handdlers       map[string][]message.Handdler
	handdlerslock   sync.RWMutex
	listenCtxCancel context.CancelFunc
	*clientkey.ClientKey
}

//NewConsumer 创建一个新的位图对象
//@params k *key.Key redis客户端的键对象
func NewConsumer(k *clientkey.ClientKey) *Consumer {
	c := new(Consumer)
	c.ClientKey = k
	c.handdlers = map[string][]message.Handdler{}
	c.handdlerslock = sync.RWMutex{}
	return c
}

//RegistHandler( 将回调函数注册到queue上
//@params topic string 注册的topic
//@params fn message.Handdler 注册到topic上的回调函数
func (c *Consumer) RegistHandler(topic string, fn message.Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	c.handdlerslock.Lock()
	_, ok := c.handdlers[topic]
	if ok {
		c.handdlers[topic] = append(c.handdlers[topic], fn)
	} else {
		c.handdlers[topic] = []message.Handdler{fn}
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
