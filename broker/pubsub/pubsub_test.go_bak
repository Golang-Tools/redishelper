package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/message"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_pubsubConsumer_Producer(t *testing.T) {
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
	q := New(cli)
	//开始测试
	q.Subscribe("test_queue", func(msg *message.Message) error {
		log.Info("get mes", log.Dict{"msg": msg})
		return nil
	})
	go q.Listen(false, "test_queue")
	defer q.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err = q.Publish(ctx, []byte(fmt.Sprintf("test-%d", ele)), "test_queue")
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	time.Sleep(time.Second)
}

func Test_pubsubConsumerPartten_Producer(t *testing.T) {
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
	q := New(cli)
	//开始测试
	q.Subscribe("test_queue", func(msg *message.Message) error {
		log.Info("get mes", log.Dict{"msg": msg})
		return nil
	})
	q.Subscribe("test_*", func(msg *message.Message) error {
		log.Info("get partten mes", log.Dict{"msg": msg})
		return nil
	})
	go q.ListenParttens(false, "test_*")
	defer q.StopListening()
	for _, ele := range []int{1, 2, 3} {
		time.Sleep(time.Second)
		err = q.Publish(ctx, []byte(fmt.Sprintf("test-%d", ele)), "test_queue")
		if err != nil {
			assert.Error(t, err, "queue put error")
		}
	}
	time.Sleep(time.Second)
}
