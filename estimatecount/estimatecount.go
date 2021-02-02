//Package estimatecount 面向对象的计数估计类型
//estimatecount 用于粗略统计大量数据去重后的个数,一般用在日活计算
package estimatecount

import (
	"time"

	"github.com/go-redis/redis/v8"
)

//EstimateCount 估计计数对象
type EstimateCount struct {
	Key    string
	MaxTTL time.Duration
	client redis.UniversalClient
}
