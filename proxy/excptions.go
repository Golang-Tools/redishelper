package proxy

import (
	"errors"
)

//ErrProxyAllreadySettedUniversalClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedUniversalClient = errors.New("代理不能重复设置客户端对象")

//ErrProxyNotYetSettedUniversalClient 代理还未设置客户端对象
var ErrProxyNotYetSettedUniversalClient = errors.New("代理还未设置客户端对象")

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")
