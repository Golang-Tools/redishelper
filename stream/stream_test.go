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
			assert.Error(t, err, "queue put error")
		}
		log.Info("send mes", log.Dict{"id": id})
	}
	time.Sleep(time.Second)
}

// func Test_streamConsumer_Subscribe(t *testing.T) {
// 	proxy := New()
// 	err := proxy.InitFromURL(TEST_REDIS_URL)
// 	defer proxy.Close()
// 	if err != nil {
// 		assert.Error(t, err, "init from url error")
// 	}
// 	conn, err := proxy.GetConn()
// 	if err != nil {
// 		assert.Error(t, err, "GetConn error")
// 	}
// 	_, err = conn.FlushDB().Result()
// 	if err != nil {
// 		assert.Error(t, err, "FlushDB error")
// 	}
// 	go func() {
// 		producer := proxy.NewStreamProducer("test_stream", 10, false)
// 		time.Sleep(1 * time.Second)
// 		_, err := producer.Publish(map[string]interface{}{"a": 1})
// 		if err != nil {
// 			assert.Error(t, err, "Producer error")
// 		}
// 	}()

// 	ctx := context.Background()
// 	consumer := proxy.NewStreamConsumer([]string{"test_stream"}, "$", 1, 3, "", false)
// 	assert.Equal(t, 3*time.Second, consumer.Block)
// 	go func() {
// 		time.Sleep(10 * time.Second)
// 		err := consumer.Close()
// 		if err != nil {
// 			assert.Error(t, err, "close error")
// 		}
// 	}()
// 	ch, err := consumer.Subscribe(ctx)
// 	if err != nil {
// 		assert.Error(t, err, "Subscribe error")
// 	}
// 	for msg := range ch {
// 		assert.Equal(t, "test_stream", msg.Stream)
// 	}
// }
