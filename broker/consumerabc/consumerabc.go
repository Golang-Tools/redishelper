package consumerabc

import (
	"sync"

	log "github.com/Golang-Tools/loggerhelper"
	"github.com/Golang-Tools/redishelper/broker/event"
)

type ConsumerABC struct {
	Handdlers     map[string][]event.Handdler
	Handdlerslock sync.RWMutex
}

//RegistHandler 将回调函数注册到queue上
//@params topic string 注册的topic,topic可以是具体的key也可以是*,表示监听所有消息
//@params fn event.Handdler 注册到topic上的回调函数
func (c *ConsumerABC) RegistHandler(topic string, fn event.Handdler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	c.Handdlerslock.Lock()
	_, ok := c.Handdlers[topic]
	if ok {
		c.Handdlers[topic] = append(c.Handdlers[topic], fn)
	} else {
		c.Handdlers[topic] = []event.Handdler{fn}
	}
	c.Handdlerslock.Unlock()
	return nil
}

//UnRegistHandler 删除特定topic上注册的回调函数
//@params topic string 要取消注册回调的topic,注意`*`取消的只是`*`类型的回调并不是全部取消,要全部取消请使用空字符串
func (c *ConsumerABC) UnRegistHandler(topic string) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	c.Handdlerslock.Lock()
	if topic == "" {
		c.Handdlers = map[string][]event.Handdler{}
	} else {
		_, ok := c.Handdlers[topic]
		if ok {
			delete(c.Handdlers, topic)
		}
	}

	c.Handdlerslock.Unlock()
	return nil
}

func (c *ConsumerABC) HanddlerEvent(asyncHanddler bool, topic string, evt *event.Event) {
	c.Handdlerslock.RLock()
	allevthanddlers, ok := c.Handdlers["*"]
	if ok {
		if asyncHanddler {
			for _, handdler := range allevthanddlers {
				go func(handdler event.Handdler) {
					err := handdler(evt)
					if err != nil {
						log.Error("message handdler get error", log.Dict{"err": err})
					}
				}(handdler)
			}
		} else {
			for _, handdler := range allevthanddlers {
				err := handdler(evt)
				if err != nil {
					log.Error("message handdler get error", log.Dict{"err": err})
				}
			}
		}
	}
	handdlers, ok := c.Handdlers[topic]
	if ok {
		if asyncHanddler {
			for _, handdler := range handdlers {
				go func(handdler event.Handdler) {
					err := handdler(evt)
					if err != nil {
						log.Error("message handdler get error", log.Dict{"err": err})
					}
				}(handdler)
			}
		} else {
			for _, handdler := range handdlers {
				err := handdler(evt)
				if err != nil {
					log.Error("message handdler get error", log.Dict{"err": err})
				}
			}
		}
	}
	c.Handdlerslock.RUnlock()
}
