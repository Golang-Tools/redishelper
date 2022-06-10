package keyspace_notifications

import (
	"fmt"
	"strings"
)

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
	EventName string                `json:"event_name"` //事件名可以参考<http://redisdoc.com/topic/notification.html>
}

func (event *NotificationEvent) Topic() string {
	if event.Type == KeySpaceNotification {
		return fmt.Sprintf("__keyspace@%s__:%s", event.DB, event.Key)
	}
	return fmt.Sprintf("__keyevent@%s__:%s", event.DB, event.EventName)
}

func FromTopicToNotificationEvent(topic, value string) (*NotificationEvent, error) {
	evt := NotificationEvent{}
	if strings.Contains(topic, "__keyspace@") {
		info := strings.ReplaceAll(topic, "__keyspace@", "")
		dbekey := strings.Split(info, "__:")
		if len(dbekey) != 2 {
			return nil, fmt.Errorf("notification topic unsupport,%s", topic)
		}
		evt.Type = KeySpaceNotification
		evt.DB = dbekey[0]
		evt.Key = dbekey[1]
		evt.EventName = value
		return &evt, nil
	} else if strings.Contains(topic, "__keyevent@") {
		info := strings.ReplaceAll(topic, "__keyevent@", "")
		dbekey := strings.Split(info, "__:")
		if len(dbekey) != 2 {
			return nil, fmt.Errorf("notification topic unsupport,%s", topic)
		}
		evt.Type = KeyEventNotification
		evt.DB = dbekey[0]
		evt.Key = value
		evt.EventName = dbekey[1]
		return &evt, nil
	} else {
		return nil, fmt.Errorf("topic %s not match spacenotification format", topic)
	}
}

// //Handdler 处理消息的回调函数
// //@params msg *NotificationEvent Event对象
// type NotificationHanddler func(evt *NotificationEvent) error
