package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	log "github.com/Golang-Tools/loggerhelper"
	message "github.com/Golang-Tools/redishelper/message"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func Test_queue_put(t *testing.T) {
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
	err = q.Publish(ctx, []byte("test1"), "test_queue")
	if err != nil {
		assert.Error(t, err, "queue put error")
	}
	res, err := q.Len(ctx, "test_queue")
	if err != nil {
		assert.Error(t, err, "queue len error")
	}
	assert.Equal(t, int64(1), res)
}

func Test_queue_listen(t *testing.T) {
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

// func Test_queueConsumer_Subscribe(t *testing.T) {
// 	// 准备工作
// 	options, err := redis.ParseURL(TEST_REDIS_URL)
// 	if err != nil {
// 		assert.Error(t, err, "init from url error")
// 	}
// 	cli := redis.NewClient(options)
// 	defer cli.Close()

// 	ctx := context.Background()
// 	_, err = cli.FlushDB(ctx).Result()
// 	if err != nil {
// 		assert.Error(t, err, "FlushDB error")
// 	}
// 	q := NewQueue(cli, "test_queue")

// 	//开始测试
// 	go func() {
// 		producer := proxy.NewQueueProducer("test_queue")
// 		time.Sleep(1 * time.Second)
// 		_, err := producer.Publish("test")
// 		if err != nil {
// 			assert.Error(t, err, "Producer error")
// 		}
// 	}()
// 	consumer := proxy.NewQueueConsumer([]string{"test_queue"})
// 	go func() {
// 		time.Sleep(10 * time.Second)
// 		err := consumer.Close()
// 		if err != nil {
// 			assert.Error(t, err, "close error")
// 		}
// 	}()
// 	ch, err := consumer.Subscribe()
// 	if err != nil {
// 		assert.Error(t, err, "Subscribe error")
// 	}
// 	for msg := range ch {
// 		assert.Equal(t, "test_queue", msg.Topic)
// 	}
// }
