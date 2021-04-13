package ext

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// TEST_REDIS_URL 测试用的redis地址
const TEST_REDIS_URL = "redis://localhost:6379"

func NewBackground(t *testing.T, URL string) (redis.UniversalClient, context.Context) {
	options, err := redis.ParseURL(URL)
	if err != nil {
		assert.FailNow(t, err.Error(), "init from url error")
	}
	cli := redis.NewClient(options)
	ctx := context.Background()
	cli.FlushDB(ctx).Result()
	res, err := cli.ConfigSet(ctx, "notify-keyspace-events", "").Result()
	if err != nil {
		fmt.Printf("reset config res:%s err:%s \n", res, err.Error())
	} else {
		fmt.Printf("reset config res:%s\n", res)
	}

	// _, err = cli.FlushDB(ctx).Result()
	// if err != nil {
	// 	assert.FailNow(t, err.Error(), "FlushDB error")
	// }
	fmt.Println("prepare task done")
	return cli, ctx
}

//Test_KeyspaceNotification_Sync 测试同步配置
func Test_Module_List(t *testing.T) {
	cli, ctx := NewBackground(t, TEST_REDIS_URL)
	defer cli.Close()
	ms, err := ListModule(cli, ctx)
	if err != nil {
		assert.FailNow(t, err.Error(), "ListModule error")
	}
	m := ms[0]
	assert.Equal(t, m.Name, "redis-cell")
	assert.Equal(t, m.Version, int64(1))
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
