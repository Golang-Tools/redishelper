package pchelper

import (
	"errors"
)

//ErrUnSupportSerializeProtocol 未支持的序列化协议
var ErrUnSupportSerializeProtocol = errors.New("unsupported serialize protocol")

//ErrNotSupportSliceAsPayload 不支持slice作为负载
var ErrNotSupportSliceAsPayload = errors.New("not support slice as payload")

//ErrMapPayloadCanNotCast map数据不能被转换
var ErrMapPayloadCanNotCast = errors.New("payload can not cast map as map[string]interface{}")

//ErrNotSupportChanAsPayload chan数据不能作为payload
var ErrNotSupportChanAsPayload = errors.New("not support chan as payload")
