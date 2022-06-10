package clientIdhelper

import (
	"github.com/Golang-Tools/idgener/machineid"
	"github.com/Golang-Tools/optparams"
)

type Options struct {
	ClientID string //客户端id
}

var DefaultOptions = Options{
	ClientID: machineid.MachineIDStr,
}

//WithClientID 中间件通用设置,设置客户端id
func WithClientID(clientID string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.ClientID = clientID
	})
}
