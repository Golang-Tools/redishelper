package redisproxy

import (
	"errors"
)

//ErrProxyAllreadySettedUniversalClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedUniversalClient = errors.New("proxy allready setted universal client")

//ErrProxyNotYetSettedUniversalClient 代理还未设置客户端对象
var ErrProxyNotYetSettedUniversalClient = errors.New("proxy notSet universal client yet")

//ErrUnknownClientType 未知的redis客户端类型
var ErrUnknownClientType = errors.New("unknown client type")
