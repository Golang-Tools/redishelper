//Package keyspace_notifications对象
package keyspace_notifications

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/go-redis/redis/v8"
)

//KeyspaceNotification 键空间通知的配置
type KeyspaceNotification struct {
	Client        redis.UniversalClient
	Conf          string
	Handdlers     map[string][]NotificationHanddler
	Handdlerslock sync.RWMutex
	listening     *redis.PubSub
}

//New 创建一个键空间通知对象
func New(client redis.UniversalClient) *KeyspaceNotification {
	k := new(KeyspaceNotification)
	k.Client = client
	k.Handdlers = map[string][]NotificationHanddler{}
	k.Handdlerslock = sync.RWMutex{}
	return k
}

//Sync 将服务器的设置与本对象进行同步
//@local bool 为true则以本地为准,否则以服务端为准
func (c *KeyspaceNotification) Sync(ctx context.Context, local bool) error {
	if local {
		res, err := c.Client.ConfigSet(ctx, "notify-keyspace-events", c.Conf).Result()
		if err != nil {
			return err
		}
		if strings.Contains(res, "OK") {
			return nil
		} else {
			return errors.New(res)
		}

	} else {
		res, err := c.Client.ConfigGet(ctx, "notify-keyspace-events").Result()
		if err != nil {
			return err
		}
		if len(res) != 2 {
			return errors.New("ConfigGet notify-keyspace-events not get 2 result")
		}
		c.Conf = res[1].(string)
		return nil
	}
}

//RegistHandler 将回调函数注册到指定位置上
//@params db, key, eventanem string 注册的位置,位置可以是具体的值也可以是*,*表示监听所有消息
//@params fn NotificationHanddler 注册到topic上的回调函数
func (c *KeyspaceNotification) RegistHandler(db, key, eventanem string, fn NotificationHanddler) error {
	if db == "" || key == "" || eventanem == "" {
		return errors.New("db,key,eventname不能为空")
	}
	topic := fmt.Sprintf("%s-%s-%s", db, key, eventanem)
	c.Handdlerslock.Lock()
	_, ok := c.Handdlers[topic]
	if ok {
		c.Handdlers[topic] = append(c.Handdlers[topic], fn)
	} else {
		c.Handdlers[topic] = []NotificationHanddler{fn}
	}
	c.Handdlerslock.Unlock()
	return nil
}

//UnRegistHandler 删除特定topic上注册的回调函数
//@params topic string 要取消注册回调的topic,注意`*`取消的只是`*`类型的回调并不是全部取消,要全部取消请使用空字符串
func (c *KeyspaceNotification) UnRegistHandler(db, key, eventanem string) error {
	if db == "" || key == "" || eventanem == "" {
		return errors.New("db,key,eventname不能为空")
	}
	topic := fmt.Sprintf("%s-%s-%s", db, key, eventanem)
	c.Handdlerslock.Lock()
	if topic == "" {
		c.Handdlers = map[string][]NotificationHanddler{}
	} else {
		_, ok := c.Handdlers[topic]
		if ok {
			delete(c.Handdlers, topic)
		}
	}
	c.Handdlerslock.Unlock()
	return nil
}

func (c *KeyspaceNotification) handdlerEvent(topic string, asyncHanddler bool, evt *NotificationEvent) {
	handdlers, ok := c.Handdlers[topic]
	if ok {
		if asyncHanddler {
			for _, handdler := range handdlers {
				go func(handdler NotificationHanddler) {
					err := handdler(evt)
					if err != nil {
						log.Error("message handdler get error", log.Dict{"err": err, "topic": topic})
					}
				}(handdler)
			}
		} else {
			for _, handdler := range handdlers {
				err := handdler(evt)
				if err != nil {
					log.Error("message handdler get error", log.Dict{"err": err, "topic": topic})
				}
			}
		}
	}
}

//HanddlerEvent 调用回调函数处理消息
//@params asyncHanddler bool 是否异步执行回调函数
//@params evt *event.Event 待处理的消息
func (c *KeyspaceNotification) HanddlerEvent(asyncHanddler bool, evt *NotificationEvent) {
	c.Handdlerslock.RLock()
	c.handdlerEvent("*-*-*", asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("%s-*-*", evt.DB), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("*-%s-*", evt.Key), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("*-*-%s", evt.EventName), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("%s-%s-*", evt.DB, evt.Key), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("%s-*-%s", evt.DB, evt.EventName), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("*-%s-%s", evt.Key, evt.EventName), asyncHanddler, evt)
	c.handdlerEvent(fmt.Sprintf("%s-%s-%s", evt.DB, evt.Key, evt.EventName), asyncHanddler, evt)
	c.Handdlerslock.RUnlock()
}

func (c *KeyspaceNotification) IsListening() bool {
	if c.listening != nil {
		return true
	}
	return false
}

//Listen 监听notification
//@params asyncHanddler bool 是否并行执行回调
func (c *KeyspaceNotification) Listen(asyncHanddler bool) error {
	if c.IsListening() {
		return ErrNoitificationsAlreadyListened
	}
	defer func() {
		c.listening = nil
	}()
	ctx := context.Background()
	pubsub := c.Client.PSubscribe(ctx, "__key*__:*")
	c.listening = pubsub
	ch := pubsub.Channel()
	for m := range ch {
		topic := m.Channel
		msg := m.Payload
		log.Debug("get msg", log.Dict{"msg": m})
		evt := NotificationEvent{}
		if strings.Contains(topic, "__keyspace@") {
			info := strings.ReplaceAll(topic, "__keyspace@", "")
			dbekey := strings.Split(info, "__:")
			if len(dbekey) != 2 {
				return fmt.Errorf("notification topic unsupport,%s", topic)
			}
			evt.Type = KeySpaceNotification
			evt.DB = dbekey[0]
			evt.Key = dbekey[1]
			evt.EventName = msg
		} else {
			if strings.Contains(topic, "__keyevent@") {
				info := strings.ReplaceAll(topic, "__keyevent@", "")
				dbekey := strings.Split(info, "__:")
				if len(dbekey) != 2 {
					return fmt.Errorf("notification topic unsupport,%s", topic)
				}
				evt.Type = KeyEventNotification
				evt.DB = dbekey[0]
				evt.Key = msg
				evt.EventName = dbekey[1]
			} else {
				return fmt.Errorf("unknown notification type,%s", topic)
			}
		}
		c.HanddlerEvent(asyncHanddler, &evt)
	}
	return nil
}

//StopListening 停止监听
func (c *KeyspaceNotification) StopListening() error {
	if !c.IsListening() {
		return ErrNoitificationsNotListeningYet
	}
	c.listening.Close()
	return nil
}
