//middlewarehelper 用于规范中间件的模块
//中间件指的是一些有特殊业务用途的模块,
//这个子模块是整个redishelper的一个基本组件
package clientIdhelper

import (
	"github.com/Golang-Tools/optparams"
)

//ClientIDInterface 作为客户端的接口,ClientID为识别自身的标识
type ClientIDInterface interface {
	//ClientID 查看组件设置的客户端ID
	ClientID() string
}

type ClientIDAbc struct {
	opt Options
}

func New(opts ...optparams.Option[Options]) (*ClientIDAbc, error) {
	l := new(ClientIDAbc)
	l.opt = DefaultOptions
	optparams.GetOption(&l.opt, opts...)
	return l, nil
}

//ClientID 查看组件设置的客户端ID
func (l *ClientIDAbc) ClientID() string {
	return l.opt.ClientID
}
