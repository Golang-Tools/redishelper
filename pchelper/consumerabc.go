package pchelper

import (
	"sync"

	"github.com/Golang-Tools/optparams"
)

//ConsumerABC 消费者的基类
//定义了回调函数的注册操作和执行操作
type ConsumerABC struct {
	Handdlers     map[string][]EventHanddler
	Handdlerslock sync.RWMutex
	*ProducerConsumerABC
}

func NewConsumerABC(opts ...optparams.Option[Options]) *ConsumerABC {
	l := new(ConsumerABC)
	l.Handdlers = map[string][]EventHanddler{}
	l.Handdlerslock = sync.RWMutex{}
	l.ProducerConsumerABC = New(opts...)
	return l
}

//RegistHandler 将回调函数注册到指定topic上
//@params topic string 注册的topic,topic可以是具体的key也可以是*,*表示监听所有消息
//@params fn EventHanddler 注册到topic上的回调函数
func (c *ConsumerABC) RegistHandler(topic string, fn EventHanddler) error {
	// if q.listenCtxCancel != nil {
	// 	return ErrQueueAlreadyListened
	// }
	c.Handdlerslock.Lock()
	_, ok := c.Handdlers[topic]
	if ok {
		c.Handdlers[topic] = append(c.Handdlers[topic], fn)
	} else {
		c.Handdlers[topic] = []EventHanddler{fn}
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
		c.Handdlers = map[string][]EventHanddler{}
	} else {
		_, ok := c.Handdlers[topic]
		if ok {
			delete(c.Handdlers, topic)
		}
	}

	c.Handdlerslock.Unlock()
	return nil
}

//HanddlerEvent 调用回调函数处理消息
//@params asyncHanddler bool 是否异步执行回调函数
//@params evt *Event 待处理的消息
func (c *ConsumerABC) HanddlerEvent(asyncHanddler bool, evt *Event) {
	c.Handdlerslock.RLock()
	allevthanddlers, ok := c.Handdlers["*"]
	if ok {
		if asyncHanddler {
			for _, handdler := range allevthanddlers {
				go func(handdler EventHanddler) {
					err := handdler(evt)
					if err != nil {
						logger.Error("message handdler get error", map[string]any{"err": err.Error()})
					}
				}(handdler)
			}
		} else {
			for _, handdler := range allevthanddlers {
				err := handdler(evt)
				if err != nil {
					logger.Error("message handdler get error", map[string]any{"err": err.Error()})
				}
			}
		}
	}
	handdlers, ok := c.Handdlers[evt.Topic]
	if ok {
		if asyncHanddler {
			for _, handdler := range handdlers {
				go func(handdler EventHanddler) {
					err := handdler(evt)
					if err != nil {
						logger.Error("message handdler get error", map[string]any{"err": err.Error()})
					}
				}(handdler)
			}
		} else {
			for _, handdler := range handdlers {
				err := handdler(evt)
				if err != nil {
					logger.Error("message handdler get error", map[string]any{"err": err.Error()})
				}
			}
		}
	}
	c.Handdlerslock.RUnlock()
}
