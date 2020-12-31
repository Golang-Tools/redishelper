package redishelper

import (
	"strconv"
	"strings"
	"time"

	log "github.com/Golang-Tools/loggerhelper"

	"github.com/sony/sonyflake"
)

var flake = sonyflake.NewSonyflake(sonyflake.Settings{
	StartTime: time.Date(2020, 12, 30, 0, 0, 0, 0, time.UTC),
})

//NameSpcae 带命名空间的键
type NameSpcae []string

//NewNameSpaceKey 创建一个带命名空间的key
func (n *NameSpcae) String() string {
	return strings.Join(*n, "::")
}

//Key 在命名空间基础上创建一个key
//终点字符串和命名空间之间使用`::`相连
//@params endpoints ...string key的终点字段
//如果endpoints长度为0,则随机生成一个字符串(32进制的snowflake算法结果),如果生成产生错误则会返回一个空字符串
//如果endpoints长度大于等于1,则使用`-`将其串联
func (n *NameSpcae) Key(endpoints ...string) string {
	lenEndpoints := len(endpoints)
	var endpoint string
	switch lenEndpoints {
	case 0:
		{
			id, err := flake.NextID()
			if err != nil {
				log.Error("生成snowflak错误", log.Dict{"err": err})
				return ""
			}
			endpoint = strconv.FormatUint(id, 32)
		}
	default:
		{
			endpoint = strings.Join(endpoints, "-")
		}
	}
	return n.String() + "::" + endpoint
}
