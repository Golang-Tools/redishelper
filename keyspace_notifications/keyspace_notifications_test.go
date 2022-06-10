package keyspace_notifications

import (
	"context"
	"testing"
	"time"

	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379/0"

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

//Test_KeyspaceNotification_Sync 测试同步配置
func Test_KeyspaceNotification_Sync(t *testing.T) {
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()
	nf, err := New(cli)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	err = nf.SetConf(ctx, "")
	if err != nil {
		assert.FailNow(t, err.Error(), "CheckConf error")
	}
	conf, err := nf.CheckConf(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "CheckConf error")
	}
	assert.Equal(t, "", conf)
	err = nf.SetConf(ctx, "Ex")
	if err != nil {
		assert.FailNow(t, err.Error(), "CheckConf error")
	}

	conf, err = nf.CheckConf(ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "CheckConf error")
	}
	assert.Contains(t, conf, "E")
	assert.Contains(t, conf, "x")
}

//Test_KeyspaceNotification_Listen 测试监听事件
func Test_KeyspaceNotification_Listen(t *testing.T) {
	cli, ctx := NewBackgroundClient(t)
	defer cli.Close()
	nf, err := New(cli,
		WithKeySpaceNotificationHandler(0, "a",
			func(msg *pchelper.Event) error {
				logger.Info("get msg", map[string]any{"msg": msg})
				evt, err := FromTopicToNotificationEvent(msg.Topic, msg.Payload.(string))
				if err != nil {
					logger.Error("get error", map[string]any{"err": err.Error()})
					return err
				}
				assert.Equal(t, "a", evt.Key)
				return nil
			}),
		WithKeySpaceNotificationHandler(0, "b",
			func(msg *pchelper.Event) error {
				logger.Info("get msg", map[string]any{"msg": msg})
				evt, err := FromTopicToNotificationEvent(msg.Topic, msg.Payload.(string))
				if err != nil {
					logger.Error("get error", map[string]any{"err": err.Error()})
					return err
				}
				assert.Equal(t, "b", evt.Key)
				return nil
			}),
		WithKeySpaceNotificationHandler(0, "c",
			func(msg *pchelper.Event) error {
				logger.Info("get msg", map[string]any{"msg": msg})
				evt, err := FromTopicToNotificationEvent(msg.Topic, msg.Payload.(string))
				if err != nil {
					logger.Error("get error", map[string]any{"err": err.Error()})
					return err
				}
				assert.Equal(t, "c", evt.Key)
				return nil
			}),
	)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	err = nf.SetConf(ctx, "AKE")
	if err != nil {
		assert.FailNow(t, err.Error(), "CheckConf error")
	}

	go nf.Start()
	time.Sleep(1 * time.Second)
	cli.Set(ctx, "a", 1, 1*time.Second)
	time.Sleep(1 * time.Second)
	cli.Set(ctx, "b", 2, 2*time.Second)
	time.Sleep(1 * time.Second)
	cli.Set(ctx, "c", 3, 3*time.Second)
	time.Sleep(1 * time.Second)
	err = nf.Stop()
	if err != nil {
		assert.FailNow(t, err.Error(), "Stop error")
	}
}
