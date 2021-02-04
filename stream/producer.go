package stream

import (
	"context"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/utils"
	"github.com/go-redis/redis/v8"
)

//Producer 流对象
type Producer struct {
	Key    string
	MaxTTL time.Duration
	MaxLen int64
	Strict bool //maxlen with ~
	client redis.UniversalClient
}

//NewProducerOptions 创建流对象的可选参数
type NewProducerOptions struct {
	MaxLen int64
	Strict bool
	MaxTTL time.Duration
}

//NewProducer 新建一个流对象
//@params client redis.UniversalClient 客户端对象
//@params key string 流使用的key
//@params option ...*NewProducerOptions 流的可选项
func NewProducer(client redis.UniversalClient, key string, option ...*NewProducerOptions) *Producer {
	s := new(Producer)
	s.client = client
	s.Key = key
	switch len(option) {
	case 0:
		{
			return s
		}
	case 1:
		{
			op := option[0]
			if op != nil {
				if op.MaxTTL <= 0 {
					log.Warn("Maxttl不能小于等于0,设置无效")
				} else {
					s.MaxTTL = op.MaxTTL
				}
				if op.MaxLen <= 0 {
					log.Warn("Maxlen不能小于等于0,设置无效")
				} else {
					s.MaxLen = op.MaxLen
				}
				s.Strict = op.Strict
				return s
			}
			log.Warn("option不能为nil,设置无效")
			return s
		}
	default:
		{
			log.Warn("option个数最多只能设置一个,使用第一个作为可选设置项")
			op := option[0]
			if op != nil {
				if op.MaxTTL <= 0 {
					log.Warn("Maxttl不能小于等于0,设置无效")
				} else {
					s.MaxTTL = op.MaxTTL
				}
				if op.MaxLen <= 0 {
					log.Warn("Maxlen不能小于等于0,设置无效")
				} else {
					s.MaxLen = op.MaxLen
				}
				s.Strict = op.Strict
				return s
			}
			log.Warn("option不能为nil,设置无效")
			return s
		}
	}
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
//@params ctx context.Context 请求的上下文
//@params topics []string 要刷新的主题列表
func (s *Producer) RefreshTTL(ctx context.Context) error {
	if s.MaxTTL != 0 {
		_, err := s.client.Expire(ctx, s.Key, s.MaxTTL).Result()
		if err != nil {
			if err != redis.Nil {
				return nil
			}
			return err
		}
		return nil
	}
	return utils.ErrKeyNotSetMaxTLL
}

//TTL 查看key的剩余时间
//@params ctx context.Context 请求的上下文
func (s *Producer) TTL(ctx context.Context) (time.Duration, error) {
	res, err := s.client.TTL(ctx, s.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, utils.ErrKeyNotExist
		}
		return 0, err
	}
	return res, nil
}

//Publish 向流发送消息
//@params ctx context.Context 请求的上下文
//@params value map[string]interface{} 发送的消息负载
//@return id string 发送出去后获得的id
func (s *Producer) Publish(ctx context.Context, value map[string]interface{}) (string, error) {
	args := redis.XAddArgs{
		Stream: s.Key,
		Values: value,
		ID:     "*",
	}
	if s.MaxLen != 0 {
		if s.Strict {
			args.MaxLen = s.MaxLen
		} else {
			args.MaxLenApprox = s.MaxLen
		}
	}
	id, err := s.client.XAdd(ctx, &args).Result()
	if err != nil {
		log.Error("publish error", log.Dict{"err": err.Error()})
	}
	return id, err
}
