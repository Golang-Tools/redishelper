package keyspace_notifications

import "fmt"

//NotificationEventType 通知类型
type NotificationEventType uint16

const (

	//KeySpaceNotification 键空间通知
	KeySpaceNotification NotificationEventType = iota
	//KeyEventNotification 键事件通知
	KeyEventNotification
)

//Options

type NotificationEvent struct {
	Type      NotificationEventType `json:"type"`
	DB        string                `json:"db"`
	Key       string                `json:"key"`
	EventName string                `json:"event_name"`
}

func (event *NotificationEvent) String() {
	fmt.Sprintf("%s-%s-%s", event.DB, event.Key, event.EventName)
}

//Handdler 处理消息的回调函数
//@params msg *NotificationEvent Event对象
type NotificationHanddler func(evt *NotificationEvent) error
