package streamhelper

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

func Test_stream_listen(t *testing.T) {
	// 准备工作
	topic := "test_stream"
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
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, fmt.Sprintf("test-%d", ele))
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []bool{true, false} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	for _, ele := range []float32{0.1, 0.2, 0.3} {
		time.Sleep(time.Second)
		err := p.Publish(ctx, topic, ele)
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_json_event_listen(t *testing.T) {
	// 准备工作
	topic := "test_stream"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	c, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5))
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
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_msgpack_event_listen(t *testing.T) {
	// 准备工作
	topic := "test_stream"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}
	c, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), SerializeWithMsgpack())
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
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen(t *testing.T) {
	// 准备工作
	topic := "test_stream"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}

	s := NewStream(ck, topic)
	cgs, err := s.CreateGroup(ctx, "group1", WithAutocreate())
	if err != nil {
		assert.Error(t, err, "CreateGroup error")
	}
	log.Info("CreateGroup get result", log.Dict{"cgs": cgs})
	//初始化消费者
	c1, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client1"))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c2, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client2"))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c3, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client3"))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c1.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.ClientID()})
		return nil
	})
	go c1.Listen(topic)
	defer c1.StopListening()

	c2.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.ClientID()})
		return nil
	})
	go c2.Listen(topic)
	defer c2.StopListening()

	c3.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.ClientID()})
		return nil
	})
	go c3.Listen(topic)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, topic, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen_with_AckWhenDone(t *testing.T) {
	// 准备工作
	topic := "test_stream"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}

	s := NewStream(ck, topic)
	cgs, err := s.CreateGroup(ctx, "group1", WithAutocreate())
	if err != nil {
		assert.Error(t, err, "CreateGroup error")
	}
	log.Info("CreateGroup get result", log.Dict{"cgs": cgs})
	//初始化消费者
	c1, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client1"), WithConsumerAckMode(AckModeAckWhenDone))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c2, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client2"), WithConsumerAckMode(AckModeAckWhenDone))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c3, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client3"), WithConsumerAckMode(AckModeAckWhenDone))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c1.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.ClientID()})
		return nil
	})
	go c1.Listen(topic)
	defer c1.StopListening()

	c2.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.ClientID()})
		return nil
	})
	go c2.Listen(topic)
	defer c2.StopListening()

	c3.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.ClientID()})
		return nil
	})
	go c3.Listen(topic)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, topic, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_stream_event_group_listen_with_NoAck(t *testing.T) {
	// 准备工作
	topic := "test_stream"
	ck, ctx := NewBackgroundClient(t)
	defer ck.Close()
	p, err := NewProducer(ck)
	if err != nil {
		assert.FailNow(t, err.Error(), "NewProducer get error")
	}

	s := NewStream(ck, topic)
	cgs, err := s.CreateGroup(ctx, "group1", WithAutocreate())
	if err != nil {
		assert.Error(t, err, "CreateGroup error")
	}
	log.Info("CreateGroup get result", log.Dict{"cgs": cgs})
	//初始化消费者
	//初始化消费者
	c1, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client1"), WithConsumerAckMode(AckModeNoAck))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c2, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client2"), WithConsumerAckMode(AckModeNoAck))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}
	c3, err := NewConsumer(ck, WithBlockTime(10*time.Second), WithConsumerRecvBatchSize(5), WithConsumerGroupName("group1"), WithClientID("client3"), WithConsumerAckMode(AckModeNoAck))
	if err != nil {
		assert.FailNow(t, err.Error(), "NewConsumer get error")
	}

	//开始测试
	c1.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c1.ClientID()})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c1.Listen(topic)
	defer c1.StopListening()

	c2.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c2.ClientID()})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c2.Listen(topic)
	defer c2.StopListening()

	c3.RegistHandler(topic, func(evt *pchelper.Event) error {
		log.Info("get event", log.Dict{"evt": evt, "reciver": c3.ClientID()})
		s.Ack(ctx, c1.opt.Group, evt.EventID)
		return nil
	})
	go c3.Listen(topic)
	defer c3.StopListening()

	for _, ele := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		time.Sleep(time.Second)
		_, err := p.PubEvent(ctx, topic, map[string]interface{}{"getnbr": ele})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
	}
	time.Sleep(time.Second)
}
