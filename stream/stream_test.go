package redishelper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_streamConsumer_Read(t *testing.T) {
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
		proxy := New()
		err := proxy.InitFromURL(TEST_REDIS_URL)
		defer proxy.Close()
		producer := proxy.NewStreamProducer("test_stream", 10, false)
		for _, v := range []int{1, 2, 3} {
			time.Sleep(1 * time.Second)
			_, err = producer.Publish(map[string]interface{}{"a": v})
			if err != nil {
				assert.Error(t, err, "Producer error")
			}
		}
	}()

	consumer := proxy.NewStreamConsumer([]string{"test_stream"}, "$", 1, 3, "", false)
	assert.Equal(t, 3*time.Second, consumer.Block)
	ctx := context.Background()
	res, err := consumer.Read(ctx)
	if err != nil {
		assert.Error(t, err, "consumer.Subscribe error")
	}
	assert.Equal(t, "test_stream", res[0].Stream)
}

func Test_streamConsumer_Subscribe(t *testing.T) {
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
		producer := proxy.NewStreamProducer("test_stream", 10, false)
		time.Sleep(1 * time.Second)
		_, err := producer.Publish(map[string]interface{}{"a": 1})
		if err != nil {
			assert.Error(t, err, "Producer error")
		}
	}()

	ctx := context.Background()
	consumer := proxy.NewStreamConsumer([]string{"test_stream"}, "$", 1, 3, "", false)
	assert.Equal(t, 3*time.Second, consumer.Block)
	go func() {
		time.Sleep(10 * time.Second)
		err := consumer.Close()
		if err != nil {
			assert.Error(t, err, "close error")
		}
	}()
	ch, err := consumer.Subscribe(ctx)
	if err != nil {
		assert.Error(t, err, "Subscribe error")
	}
	for msg := range ch {
		assert.Equal(t, "test_stream", msg.Stream)
	}
}
