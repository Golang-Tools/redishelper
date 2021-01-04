package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_queueConsumer_Queue(t *testing.T) {
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
	q := proxy.NewQueue("test_queue")
	time.Sleep(1 * time.Second)
	_, err = q.Put("test1", "test2")
	if err != nil {
		assert.Error(t, err, "queue put error")
	}
	res, err := q.Len()
	if err != nil {
		assert.Error(t, err, "queue len error")
	}
	assert.Equal(t, int64(2), res)

}
func Test_queueConsumer_Subscribe(t *testing.T) {
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
		producer := proxy.NewQueueProducer("test_queue")
		time.Sleep(1 * time.Second)
		_, err := producer.Publish("test")
		if err != nil {
			assert.Error(t, err, "Producer error")
		}
	}()
	consumer := proxy.NewQueueConsumer([]string{"test_queue"})
	go func() {
		time.Sleep(10 * time.Second)
		err := consumer.Close()
		if err != nil {
			assert.Error(t, err, "close error")
		}
	}()
	ch, err := consumer.Subscribe()
	if err != nil {
		assert.Error(t, err, "Subscribe error")
	}
	for msg := range ch {
		assert.Equal(t, "test_queue", msg.Topic)
	}
}
