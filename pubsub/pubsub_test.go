package redishelper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_pubsubConsumer_Subscribe(t *testing.T) {
	proxy := New()
	err := proxy.InitFromURL(TEST_REDIS_URL)
	defer proxy.Close()
	if err != nil {
		assert.Error(t, err, "init from url error")
	}
	conn, err := proxy.GetConn()
	if err != nil {
		assert.Error(t, err, "GetConn error")
	}
	_, err = conn.FlushDB().Result()
	if err != nil {
		assert.Error(t, err, "FlushDB error")
	}
	go func() {

		producer := proxy.NewPubSubProducer("test_pubsub")
		time.Sleep(1 * time.Second)
		_, err := producer.Publish("test")
		if err != nil {
			assert.Error(t, err, "Producer error")
		}
	}()

	ctx := context.Background()
	consumer := proxy.NewPubSubConsumer([]string{"test_pubsub"})

	go func() {
		time.Sleep(10 * time.Second)
		consumer.UnSubscribe(consumer.Topics...)
		consumer.Close()
	}()
	ch, err := consumer.Subscribe(ctx)
	if err != nil {
		assert.Error(t, err, "Subscribe error")
	}
	for msg := range ch {
		assert.Equal(t, "test_pubsub", msg.Channel)
	}
}
