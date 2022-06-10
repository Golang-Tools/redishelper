package queuehelper

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackgroundClient(t *testing.T) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.FailNow(t, err.Error(), "FlushDB error")
	}
	return cli, ctx
}

func Test_queue_put(t *testing.T) {
	// 准备工作
	topic := "test_queue"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	//开始测试
	err = p.Publish(ctx, topic, []byte("test1"))
	if err != nil {
		assert.Error(t, err, "queue put error")
	}
	res, err := p.Len(ctx, topic)
	if err != nil {
		assert.Error(t, err, "queue len error")
	}
	assert.Equal(t, int64(1), res)
}

func Test_queue_listen(t *testing.T) {
	// 准备工作
	topic := "test_queue"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	c, err := NewConsumer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(topic)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, []byte(fmt.Sprintf("test-%d", ele)))
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, fmt.Sprintf("test-%d", ele))
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	for _, ele := range []bool{true, false} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	for _, ele := range []float32{0.1, 0.2, 0.3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_queue_json_event_listen(t *testing.T) {
	// 准备工作
	topic := "test_queue"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	c, err := NewConsumer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(topic)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, topic, map[string]any{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_queue_msgpack_event_listen(t *testing.T) {
	// 准备工作
	topic := "test_queue"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	c, err := NewConsumer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(topic)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, topic, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	time.Sleep(time.Second)
}
