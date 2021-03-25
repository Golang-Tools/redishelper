package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_stream_Consumer_Producer(t *testing.T) {
	// 准备工作
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	cli := redis.NewClient(options)
	defer cli.Close()

	ctx := context.Background()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	c := NewConsumer(cli)
	p := NewProducer(cli, "teststream")
	// 开始
	c.Subscribe("teststream", func(msg *redis.XStream) error {
		log.Info("get mes", log.Dict{"msg": msg})
		return nil
	})

	go c.Listen(false, &TopicInfo{Topic: "teststream"})
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		id, err := p.Publish(ctx, map[string]interface{}{"msg": fmt.Sprintf("test-%d", ele)})
		if err != nil {
			assert.Error(t, err, "stream put error")
		}
		log.Info("send mes", log.Dict{"id": id})
	}
	time.Sleep(time.Second)
}

func Test_stream_GroupConsumer_Producer(t *testing.T) {
	// 准备工作
	options, err := redis.ParseURL(TEST_REDIS_URL)
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	cli := redis.NewClient(options)
	defer cli.Close()

	ctx := context.Background()
	_, err = cli.FlushDB(ctx).Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	s := New(cli, "teststream")
	c := NewGroupConsumer(cli, "group1", uint16(12))
	p := NewProducer(cli, "teststream")
	// 开始
	res, err := s.CreateGroup(ctx, "group1", "$")
	if err != nil {
		assert.Error(t, err, "stream CreateGroup error")
	}
	log.Info("get result", log.Dict{"res": res})
	c.Subscribe("teststream", func(msg *redis.XStream) error {
		log.Info("get mes", log.Dict{"msg": msg})
		return nil
	})

	go c.Listen(false, &TopicInfo{Topic: "teststream"})
	defer c.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		id, err := p.Publish(ctx, map[string]interface{}{"msg": fmt.Sprintf("test-%d", ele)})
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
		log.Info("send mes", log.Dict{"id": id})
	}
	time.Sleep(time.Second)
}
