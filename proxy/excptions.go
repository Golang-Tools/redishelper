package proxy

import (
	"errors"
)

//ErrProxyAllreadySettedUniversalClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedUniversalClient = errors.New("代理不能重复设置客户端对象")

//ErrProxyNotYetSettedUniversalClient 代理还未设置客户端对象
var ErrProxyNotYetSettedUniversalClient = errors.New("代理还未设置客户端对象")
