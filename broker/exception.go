package broker

import (
	"errors"
)

//ErrUnSupportSerializeProtocol 未支持的序列化协议
var ErrUnSupportSerializeProtocol = errors.New("未支持的序列化协议")
