package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/v2/broker"
	"github.com/Golang-Tools/redishelper/v2/broker/event"
	"github.com/Golang-Tools/redishelper/v2/clientkey"
	kb "github.com/Golang-Tools/redishelper/v2/clientkey/clientkeybatch"
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

func Test_stream_listen(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	p := NewProducer(s)
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck, broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
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
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, fmt.Sprintf("test-%d", ele))
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []bool{true, false} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []float32{0.1, 0.2, 0.3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_json_event_listen(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	p := NewProducer(s)
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck, broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))

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
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_msgpack_event_listen(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	p := NewProducer(s, broker.SerializeWithMsgpack())
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	c := NewConsumer(ck, broker.WithBlockTime(10*time.Second), broker.SerializeWithMsgpack(), broker.WithStreamComsumerRecvBatchSize(5))

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
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	//初始化生产者
	p := NewProducer(s)
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	// 注册group
	s.CreateGroup(ctx, "group1", "$", true)
	//初始化消费者
	c1 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(1), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c2 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(2), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c3 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(3), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	//开始测试
	c1.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.opt.ClientID})
		return nil
	})
	go c1.Listen(false)
	defer c1.StopListening()

	c2.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.opt.ClientID})
		return nil
	})
	go c2.Listen(false)
	defer c2.StopListening()

	c3.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.opt.ClientID})
		return nil
	})
	go c3.Listen(false)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen_with_AckWhenDone(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	//初始化生产者
	p := NewProducer(s)
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	// 注册group
	s.CreateGroup(ctx, "group1", "$", true)
	//初始化消费者
	c1 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(1), broker.WithStreamComsumerAckMode(broker.AckModeAckWhenDone), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c2 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(2), broker.WithStreamComsumerAckMode(broker.AckModeAckWhenDone), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c3 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(3), broker.WithStreamComsumerAckMode(broker.AckModeAckWhenDone), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	//开始测试
	c1.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.opt.ClientID})
		return nil
	})
	go c1.Listen(false)
	defer c1.StopListening()

	c2.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.opt.ClientID})
		return nil
	})
	go c2.Listen(false)
	defer c2.StopListening()

	c3.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.opt.ClientID})
		return nil
	})
	go c3.Listen(false)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen_with_NoAck(t *testing.T) {
	// 准备工作
	keyname := "test_stream"
	pk := NewBackgroundProducerKey(t, keyname)
	s := New(pk, 100, false)
	//初始化生产者
	p := NewProducer(s)
	ck, ctx := NewBackgroundConsumerKey(t, keyname)
	// 注册group
	s.CreateGroup(ctx, "group1", "$", true)
	//初始化消费者
	c1 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(1), broker.WithStreamComsumerAckMode(broker.AckModeNoAck), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c2 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(2), broker.WithStreamComsumerAckMode(broker.AckModeNoAck), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	c3 := NewConsumer(ck, broker.WithStreamComsumerGroupName("group1"), broker.WithClientID(3), broker.WithStreamComsumerAckMode(broker.AckModeNoAck), broker.WithBlockTime(10*time.Second), broker.WithStreamComsumerRecvBatchSize(5))
	//开始测试
	c1.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.opt.ClientID})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c1.Listen(false)
	defer c1.StopListening()

	c2.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.opt.ClientID})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c2.Listen(false)
	defer c2.StopListening()

	c3.RegistHandler(keyname, func(evt *event.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.opt.ClientID})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c3.Listen(false)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}
