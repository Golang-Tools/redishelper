package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker"
	"github.com/Golang-Tools/redishelper/broker/event"
	"github.com/Golang-Tools/redishelper/clientkey"
	kb "github.com/Golang-Tools/redishelper/clientkey/clientkeybatch"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackgroundProducerKey(t *testing.T, keyname string, opts ...clientkey.Option) *clientkey.ClientKey {
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
	key := clientkey.New(cli, keyname, opts...)
	fmt.Println("prepare task done")
	return key
}
func NewBackgroundConsumerKey(t *testing.T, keyname string, opts ...clientkey.Option) (*kb.ClientKeyBatch, context.Context) {
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
	key := kb.New(cli, []string{keyname}, opts...)
	fmt.Println("prepare task done")
	return key, ctx
}

func Test_pubsub_listen(t *testing.T) {
	// 准备工作
	keyname := "test_pubsub"
	pk := NewBackgroundProducerKey(t, keyname)
	p := NewProducer(pk)
	// q := p.AsQueue()
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck)

	//开始测试
	c.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(false)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, []byte(fmt.Sprintf("test-%d", ele)))
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, fmt.Sprintf("test-%d", ele))
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	for _, ele := range []bool{true, false} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	for _, ele := range []float32{0.1, 0.2, 0.3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_pubsub_json_event_listen(t *testing.T) {
	// 准备工作
	keyname := "test_pubsub"
	pk := NewBackgroundProducerKey(t, keyname)
	p := NewProducer(pk)
	// q := p.AsQueue()
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck)

	//开始测试
	c.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(false)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_pubsub_msgpack_event_listen(t *testing.T) {
	// 准备工作
	keyname := "test_pubsub"
	pk := NewBackgroundProducerKey(t, keyname)
	p := NewProducer(pk, broker.SerializeWithMsgpack())
	// q := p.AsQueue()
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck, broker.SerializeWithMsgpack())

	//开始测试
	c.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt})
		return nil
	})
	go c.Listen(false)
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "pubsub put error")
		}
	}
	time.Sleep(time.Second)
}
