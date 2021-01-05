package pubsub

import (
	"errors"
)

//ErrPubSubNotSetMaxTLL bitmap没有设置最大tll
var ErrPubSubNotSetMaxTLL = errors.New("PubSub没有设置最大tll")

//ErrPubSubAlreadyListened 队列已经被监听了
var ErrPubSubAlreadyListened = errors.New("队列已经被监听了")

//ErrPubSubNotListeningYet 队列未被监听
var ErrPubSubNotListeningYet = errors.New("队列未被监听")

//ErrNeedToPointOutTopics 需要指名队列
var ErrNeedToPointOutTopics = errors.New("需要指名队列")
