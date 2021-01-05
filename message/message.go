package message

import (
	"github.com/go-redis/redis/v8"
)

//Message 消息对象
type Message struct {
	Topic   string
	Payload []byte
}

//NewFromQueue 从queue中获取消息对象
func NewFromQueue(res []string) (*Message, error) {
	if len(res) != 2 {
		return nil, ErrQueueResNotTwo
	}
	m := new(Message)
	m.Topic = res[0]
	m.Payload = []byte(res[1])
	return m, nil
}

//NewFromRedisMessage 从pubsub中获取消息对象
func NewFromRedisMessage(msg *redis.Message) (*Message, error) {
	m := new(Message)
	m.Topic = msg.Channel
	m.Payload = []byte(msg.Payload)
	return m, nil
}

// Handdler 处理消息的回调函数
//@params msg *Message Message对象
type Handdler func(msg *Message) error
