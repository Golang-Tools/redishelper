package keyspace_notifications

import (
	"fmt"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
)

//Options broker的配置
type Options struct {
	NotificationEventHandlers map[string]pchelper.EventHanddler
	// PubsubOpts                []optparams.Option[pubsubhelper.Options]
}

//Defaultopt 默认的可选配置
var Defaultopt = Options{
	NotificationEventHandlers: map[string]pchelper.EventHanddler{},
	// PubsubOpts:                []optparams.Option[pubsubhelper.Options]{},
}

//WithKeySpaceNotificationHandler 注册监听keyspace通知的回调函数
func WithKeySpaceNotificationHandler(db int, key string, fn pchelper.EventHanddler) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.NotificationEventHandlers == nil {
			o.NotificationEventHandlers = map[string]pchelper.EventHanddler{}
		}
		topic := fmt.Sprintf("__keyspace@%d__:%s", db, key)
		o.NotificationEventHandlers[topic] = fn
	})
}

//WithKeyEventNotificationHandler 注册监听KeyEvent通知的回调函数
func WithKeyEventNotificationHandler(db int, event string, fn pchelper.EventHanddler) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.NotificationEventHandlers == nil {
			o.NotificationEventHandlers = map[string]pchelper.EventHanddler{}
		}
		topic := fmt.Sprintf("__keyevent@%d__:%s", db, event)
		o.NotificationEventHandlers[topic] = fn
	})
}
