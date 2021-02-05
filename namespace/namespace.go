package namespace

import (
	"strings"

	"github.com/Golang-Tools/redishelper/utils"
)

const DEFAULT_DELIMITER = "::"
const DEFAULT_KEY_DELIMITER = "-"

//NameSpcae 带命名空间的键
type NameSpcae []string

//ToString namespace转换为字符串
func (n *NameSpcae) ToString(delimiter ...string) (string, error) {
	switch len(delimiter) {
	case 0:
		{
			return strings.Join(*n, DEFAULT_DELIMITER), nil
		}
	case 1:
		{
			if delimiter[0] != "" {
				return strings.Join(*n, delimiter[0]), nil
			}
			return "", ErrrParamDelimiterCannotEmpty
		}
	default:
		{
			return "", ErrParamDelimiterLengthMustLessThan2
		}
	}
}

//Key 在命名空间基础上创建一个key
//@params endpoints ...string key的终点字段
//如果endpoints长度为0,则将命名空间作为key
//如果endpoints长度>=1,则将命名空间使用`::`连接,命名空间和endpoints间使用`::`连接,endpoints间使用`-`分隔
func (n *NameSpcae) Key(endpoints ...string) (string, error) {
	namespaceStr, _ := n.ToString()
	lenEndpoints := len(endpoints)
	switch lenEndpoints {
	case 0:
		{
			return namespaceStr, nil
		}
	default:
		{
			for _, endpoint := range endpoints {
				if endpoint == "" {
					return "", ErrrParamEndpointCannotEmpty
				}
			}
			endpointStr := strings.Join(endpoints, DEFAULT_KEY_DELIMITER)
			return namespaceStr + DEFAULT_DELIMITER + endpointStr, nil
		}
	}
}

//DelimiterOpt 自定义间隔符的设置
type DelimiterOpt struct {
	NamespaceDelimiter          string
	NamespaceEndpointsDelimiter string
	EndpointsDelimiter          string
}

//KeyWithDelimiter 在命名空间基础上创建一个key,可以自定义与命名空间间的间隔和endpoints间的空格
//@params opt *DelimiterOpt NamespaceDelimiter,NamespaceEndpointsDelimiter,EndpointsDelimiter如果为空字符串则使用默认值
func (n *NameSpcae) KeyWithDelimiter(opt *DelimiterOpt, endpoints ...string) (string, error) {
	nd := DEFAULT_DELIMITER
	ned := DEFAULT_DELIMITER
	ed := DEFAULT_KEY_DELIMITER
	if opt != nil {
		if opt.NamespaceDelimiter != "" {
			nd = opt.NamespaceDelimiter
		}
		if opt.NamespaceEndpointsDelimiter != "" {
			ned = opt.NamespaceEndpointsDelimiter
		}
		if opt.EndpointsDelimiter != "" {
			ed = opt.EndpointsDelimiter
		}
	}
	namespaceStr, _ := n.ToString(nd)
	lenEndpoints := len(endpoints)
	switch lenEndpoints {
	case 0:
		{
			return namespaceStr, nil
		}
	default:
		{
			for _, endpoint := range endpoints {
				if endpoint == "" {
					return "", ErrrParamEndpointCannotEmpty
				}
			}
			endpointStr := strings.Join(endpoints, ed)
			return namespaceStr + ned + endpointStr, nil
		}
	}
}

//FromKeyStr 从key字符串中解析出命名空间和终点key
func FromKeyStr(key string, opts ...*DelimiterOpt) (NameSpcae, string, error) {
	nd := DEFAULT_DELIMITER
	ned := DEFAULT_DELIMITER
	lenOpts := len(opts)
	switch lenOpts {
	case 0:
		{
		}
	case 1:
		{
			opt := opts[0]
			if opt != nil {
				if opt.NamespaceDelimiter != "" {
					nd = opt.NamespaceDelimiter
				}
				if opt.NamespaceEndpointsDelimiter != "" {
					ned = opt.NamespaceEndpointsDelimiter
				}
			}
		}
	default:
		{

			return nil, "", utils.ErrParamOptsLengthMustLessThan2
		}
	}

	ns := strings.Split(key, ned)
	if nd == ned {
		switch len(ns) {
		case 1:
			{
				return nil, "", ErrKeyNotHaveNamespace
			}
		default:
			{
				n := NameSpcae(ns[:len(ns)-1])
				k := ns[len(ns)-1]
				return n, k, nil
			}
		}
	}
	switch len(ns) {
	case 1:
		{
			return nil, "", ErrKeyNotHaveNamespace
		}
	case 2:
		{
			n := NameSpcae(strings.Split(ns[0], nd))
			k := ns[1]
			return n, k, nil
		}
	default:
		{
			return nil, "", ErrKeyParserNamespaceNumberNot2
		}
	}
}
