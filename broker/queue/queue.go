//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"

	"github.com/Golang-Tools/redishelper/v2/broker"
	"github.com/Golang-Tools/redishelper/v2/clientkey"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//Queue 消息队列
type Queue struct {
	*clientkey.ClientKey
}

//New 创建一个新的队列对象
//@params k *clientkey.ClientKey redis客户端的键对象
func New(k *clientkey.ClientKey) *Queue {
	c := new(Queue)
	c.ClientKey = k
	return c
}

// Len 查看当前队列长度
//@params ctx context.Context 请求的上下文
func (q *Queue) Len(ctx context.Context) (int64, error) {
	return q.Client.LLen(ctx, q.Key).Result()
}

func (q *Queue) AsProducer(opts ...broker.Option) *Producer {
	p := NewProducer(q.ClientKey, opts...)
	return p
}
