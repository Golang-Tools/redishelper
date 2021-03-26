//Package stream 流及相关对象的包
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/event"
	redis "github.com/go-redis/redis/v8"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

//Producer 流的生产者对象
type Producer struct {
	opt broker.Options
	*Stream
}

//NewProducer 创建一个新的流生产者
//@params k *clientkey.ClientKey redis客户端的键对象
//@params opts ...broker.Option 生产者的配置
func NewProducer(k *Stream, opts ...broker.Option) *Producer {
	c := new(Producer)
	c.Stream = k
	c.opt = broker.Defaultopt
	for _, opt := range opts {
		opt.Apply(&c.opt)
	}
	return c
}

//Publish 向流中放入数据
//@params ctx context.Context 请求的上下文
//@params payload interface{} 发送的消息负载,负载如果不是map[string]interface{}形式或者可以被json/msgpack序列化的对象则统一以[value 值]的形式传出
func (p *Producer) Publish(ctx context.Context, payload interface{}) error {
	args := redis.XAddArgs{}
	v := reflect.ValueOf(payload)
	switch v.Kind() {
	case reflect.Bool:
		{
			if payload.(bool) == true {
				args.Values = map[string]interface{}{"value": "true"}
			} else {
				args.Values = map[string]interface{}{"value": "false"}
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		{
			args.Values = map[string]interface{}{"value": payload}
		}
	case reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		{
			args.Values = map[string]interface{}{"value": payload}
		}
	case reflect.String:
		{
			args.Values = map[string]interface{}{"value": payload}
		}
	case reflect.Slice:
		{
			_, ok := payload.([]byte)
			if ok {
				args.Values = map[string]interface{}{"value": payload}
			} else {
				return errors.New("not support slice as payload")
			}
		}
	case reflect.Map:
		{
			pl, err := payload.(map[string]interface{})
			for key, value := range pl {

			}
			args.Values = payload
		}
	case reflect.Chan:
		{
			return errors.New("not support chan as payload")
		}
	default:
		{
			mm := map[string]interface{}{}
			switch p.opt.SerializeProtocol {
			case "JSON":
				{
					payloadBytes, err := json.Marshal(payload)
					if err != nil {
						return err
					}
					err = json.Unmarshal(payloadBytes, &mm)
					if err != nil {
						return err
					}
					args.Values = mm
				}
			case "msgpack":
				{
					payloadBytes, err := msgpack.Marshal(payload)
					if err != nil {
						return err
					}
					err = msgpack.Unmarshal(payloadBytes, &mm)
					if err != nil {
						return err
					}
					args.Values = mm
				}
			default:
				{
					return broker.ErrUnSupportSerializeProtocol
				}
			}
		}
	}
	if p.Strict {
		args.MaxLen = p.MaxLen
	} else {
		args.MaxLenApprox = p.MaxLen
	}
	args.Stream = p.Key
	args.ID = "*"

	_, err := p.Client.XAdd(ctx, &args).Result()
	log.Info("send msg", log.Dict{"args": args, "err": err})
	return err
}

//PubEvent 向流中放入事件数据
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

	return p.Publish(ctx, msg)
}

func (p *Producer) AsStream() *Stream {
	return p.Stream
}
