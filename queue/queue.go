//Package queue 队列对象
package queue

import (
	"context"
	"time"

	helper "github.com/Golang-Tools/redishelper"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/common/log"
)

type Message interface {
	ToBytes() ([]byte, error)
	InitFromBytes([]byte) error
}

type Handdler func(msg Message) error

//Queue 消息队列
type Queue struct {
	Key       string
	MaxTTL    time.Duration
	client    helper.GoRedisV8Client
	handdlers []Handdler
}

//New 新建一个PubSub主题
func New(client helper.GoRedisV8Client, key string, maxttl ...time.Duration) *Queue {
	q := new(Queue)
	q.Key = key
	q.client = client
	switch len(maxttl) {
	case 0:
		{
			return q
		}
	case 1:
		{
			if maxttl[0] != 0 {
				q.MaxTTL = maxttl[0]
				return q
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return q
		}
	default:
		{
			log.Warn("ttl最多只能设置一个,使用第一个作为过期时间")
			if maxttl[0] != 0 {
				q.MaxTTL = maxttl[0]
				return q
			}
			log.Warn("maxttl必须大于0,maxttl设置无效")
			return q
		}
	}
}

//RegistHanddler 将回调函数注册到queue上
func (q *Queue) RegistHanddler(fn Handdler) {
	q.handdlers = append(q.handdlers, fn)
}

//生命周期操作

//RefreshTTL 刷新key的生存时间
func (q *Queue) RefreshTTL(ctx context.Context) error {
	if q.MaxTTL != 0 {
		_, err := q.client.Exists(ctx, q.Key).Result()
		if err != nil {
			if err != redis.Nil {
				return err
			}
			return ErrKeyNotExist
		}
		_, err = q.client.Expire(ctx, q.Key, q.MaxTTL).Result()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrBitmapNotSetMaxTLL
}

//TTL 查看key的剩余时间
func (q *Queue) TTL(ctx context.Context) (time.Duration, error) {
	_, err := q.client.Exists(ctx, q.Key).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
		return 0, ErrKeyNotExist
	}
	res, err := q.client.TTL(ctx, q.Key).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

// Len 查看当前队列长度
func (q *Queue) Len(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, q.Key).Result()
}

//Put 向队列中放入数据
func (q *Queue) Put(ctx context.Context, refreshTTL bool, values ...interface{}) error {
	if refreshTTL {
		_, err := bm.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LPush(ctx, q.Key, values...)
			pipe.Expire(ctx, bm.Key, bm.MaxTTL)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := q.client.LPush(ctx, q.Key, values...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Get 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
func (q *Queue) Get(ctx context.Context, refreshTTL bool) (*QueueMessage, error) {
	if refreshTTL {
		defer bm.RefreshTTL(ctx)
	}
	res, err := q.client.BRPop(timeout, q.Name).Result()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, ErrQueueGetNotMatch
	}
	m := &Message{
		Topic:   res[0],
		Payload: []byte(res[1]),
	}
	return m, nil
}

//GetNoWait 从队列中取出数据,timeout为0则表示一直阻塞直到有数据
func (q *Queue) GetNoWait(ctx context.Context, refreshTTL bool) (*QueueMessage, error) {
	if refreshTTL {
		defer bm.RefreshTTL(ctx)
	}
	res, err := q.client.RPop(q.Name).Result()
	if err != nil {
		return nil, err
	}
	m := &QueueMessage{
		Topic:   q.Name,
		Payload: res,
	}
	return m, nil
}

func Listen(asyncHanddler bool) {

}
