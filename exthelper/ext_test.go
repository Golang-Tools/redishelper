package exthelper

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
