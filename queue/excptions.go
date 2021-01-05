package queue

import (
	"errors"
)

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")

//ErrTopicNotExist key不存在
var ErrTopicNotExist = errors.New("topic不存在")

//ErrQueueNotSetMaxTLL bitmap没有设置最大tll
var ErrQueueNotSetMaxTLL = errors.New("Queue没有设置最大tll")

//ErrQueueAlreadyListened 队列已经被监听了
var ErrQueueAlreadyListened = errors.New("队列已经被监听了")

//ErrQueueNotListeningYet 队列未被监听
var ErrQueueNotListeningYet = errors.New("队列未被监听")

//ErrNeedToPointOutTopics 需要指名队列
var ErrNeedToPointOutTopics = errors.New("需要指名队列")
