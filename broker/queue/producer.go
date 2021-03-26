//Package queue 队列对象
//非常适合作为简单的生产者消费者模式的中间件
package queue

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey"
	"github.com/Golang-Tools/redishelper/randomkey"
	uuid "github.com/satori/go.uuid"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

//Producer 队列的生产者对象
type Producer struct {
	*clientkey.ClientKey
	opt broker.Options
}

//NewProducer 创建一个新的队列生产者对象
//@params k *clientkey.ClientKey redis客户端的键对象
//@params opts ...broker.Option 生产者的配置
func NewProducer(k *clientkey.ClientKey, opts ...broker.Option) *Producer {
	c := new(Producer)
	c.ClientKey = k
	c.opt = broker.Defaultopt
	for _, opt := range opts {
		opt.Apply(&c.opt)
	}
	return c
}

//Publish 向队列中放入数据
//@params ctx context.Context 请求的上下文
//@params payload interface{} 发送的消息负载,负载支持string,bytes,bool,number,以及可以被json或者msgpack序列化的对象
func (p *Producer) Publish(ctx context.Context, payload interface{}) error {
	switch payload.(type) {
	case string, []byte:
		{
			p.Client.LPush(ctx, p.Key, payload).Result()
		}
	case bool:
		{
			if payload.(bool) == true {
				p.Client.LPush(ctx, p.Key, "true").Result()
			} else {
				p.Client.LPush(ctx, p.Key, "false").Result()
			}
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		{
			p.Client.LPush(ctx, p.Key, payload).Result()
		}
	case float32, float64, complex64, complex128:
		{
			p.Client.LPush(ctx, p.Key, payload).Result()
		}
	default:
		{
			switch p.opt.SerializeProtocol {
			case "JSON":
				{
					payloadBytes, err := json.Marshal(payload)
					if err != nil {
						return err
					}
					p.Client.LPush(ctx, p.Key, payloadBytes).Result()
				}
			case "msgpack":
				{
					payloadBytes, err := msgpack.Marshal(payload)
					if err != nil {
						return err
					}
					p.Client.LPush(ctx, p.Key, payloadBytes).Result()
				}
			default:
				{
					return broker.ErrUnSupportSerializeProtocol
				}
			}
		}
	}

	if p.Opt.MaxTTL != 0 {
		err := p.RefreshTTL(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

//PubEvent 向队列中放入事件数据
//@params ctx context.Context 请求的上下文
//@params payload []byte 发送的消息负载
func (p *Producer) PubEvent(ctx context.Context, payload interface{}) error {

	msg := event.Event{
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
		msg.EventID = mid
	} else {
		msg.EventID = hex.EncodeToString(uuid.NewV4().Bytes())
	}
	return p.Publish(ctx, msg)
}

func (p *Producer) AsQueue() *Queue {
	q := New(p.ClientKey)
	return q
}
