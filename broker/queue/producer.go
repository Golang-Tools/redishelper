//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/message"
	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/randomkey"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	uuid "github.com/satori/go.uuid"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

//Producer 队列的生产者对象
type Producer struct {
	*clientkey.ClientKey
	opt broker.Options
}

//NewProducer 创建一个新的位图对象
//@params k *key.Key redis客户端的键对象
func NewProducer(k *clientkey.ClientKey, opts ...broker.Option) *Producer {
	c := new(Producer)
	c.ClientKey = k
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

//Publish 向队列中放入数据
//@params ctx context.Context 请求的上下文
//@params payload []byte 发送的消息负载
func (p *Producer) Publish(ctx context.Context, payload interface{}) error {

	msg := message.Message{
		Topic:     p.Key,
		EventTime: time.Now().Unix(),
		Payload:   payload,
	}
	if p.opt.ClientID != 0 {
		msg.Sender = strconv.FormatUint(uint64(p.opt.ClientID), 16)
	}
	if p.opt.UUIDType == "sonyflake" {
		mid, err := randomkey.Next()
		if err != nil {
			return err
		}
		msg.MessageID = mid
	} else {
		msg.MessageID = hex.EncodeToString(uuid.NewV4().Bytes())
	}
	if p.opt.SerializeProtocol == "JSON" {
		json.Marshal(msg)
	}
	p.Client.LPush(ctx, p.Key, payload).Result()

	_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range topics {
			pipe.LPush(ctx, key, payload)
			if q.MaxTTL != 0 {
				pipe.Expire(ctx, key, q.MaxTTL)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
