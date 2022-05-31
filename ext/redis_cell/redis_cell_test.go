package redis_cell

import (
	"context"
	"fmt"
	"testing"

	"github.com/Golang-Tools/redishelper/v2/clientkey"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, URL string, keyname string, opts ...clientkey.Option) (*clientkey.ClientKey, context.Context) {
	options, err := redis.ParseURL(URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	key := clientkey.New(cli, keyname, opts...)
	// _, err = cli.FlushDB(ctx).Result()
	// if err != nil {
	// 	assert.FailNow(t, err.Error(), "FlushDB error")
	// }
	fmt.Println("prepare task done")
	return key, ctx
}

//Test_KeyspaceNotification_Sync 测试同步配置
func Test_ClThrottle(t *testing.T) {
	key, ctx := NewBackground(t, TEST_REDIS_URL, "test_redis-cell")
	defer key.Client.Close()
	cell, err := New(key)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	res, err := cell.ClThrottle(ctx, 1)
	if err != nil {
		assert.FailNow(t, err.Error(), "New error")
	}
	fmt.Println(res)
	assert.Equal(t, res.Blocked, false)
	assert.Equal(t, res.Max, cell.opt.MaxBurst+1)
}

// //Test_KeyspaceNotification_Sync 测试监听事件
// func Test_KeyspaceNotification_Listen(t *testing.T) {
// 	cli, ctx := NewBackground(t, TEST_REDIS_URL)
// 	defer cli.Close()
// 	nf := New(cli)
// 	nf.Conf = "Ex"
// 	nf.Sync(ctx, true)
// 	nf.RegistHandler("*", "*", "*", func(evt *NotificationEvent) error {
// 		fmt.Println(evt)
// 		assert.Contains(t, []string{"a", "b", "c"}, evt.Key)
// 		return nil
// 	})
// 	go nf.Listen(true)
// 	cli.Set(ctx, "a", 1, 1*time.Second)
// 	cli.Set(ctx, "b", 2, 2*time.Second)
// 	cli.Set(ctx, "c", 3, 3*time.Second)
// 	time.Sleep(4 * time.Second)
// }
