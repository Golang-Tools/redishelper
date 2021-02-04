package namespace

import (
	"strings"
)

//NameSpcae 带命名空间的键
type NameSpcae []string

//NewNameSpaceKey 创建一个带命名空间的key
func (n *NameSpcae) String() string {
	return strings.Join(*n, "::")
}

//Key 在命名空间基础上创建一个key
//终点字符串和命名空间之间使用`::`相连
//@params endpoints ...string key的终点字段
//如果endpoints长度为0,则将命名空间作为key
//如果endpoints长度大于等于1,则使用`-`将其串联
func (n *NameSpcae) Key(endpoints ...string) string {
	lenEndpoints := len(endpoints)
	var endpoint string
	switch lenEndpoints {
	case 0:
		{
			return n.String()
		}
	default:
		{
			endpoint = strings.Join(endpoints, "-")
			return n.String() + "::" + endpoint
		}
	}
}
