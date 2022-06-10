package pubsubhelper

import (
	"errors"
)

//ErrPubSubAlreadyListened 发布订阅器已经被监听了
var ErrPubSubAlreadyListened = errors.New("pubsub already listened")

//ErrPubSubNotListeningYet 发布订阅器未被监听
var ErrPubSubNotListeningYet = errors.New("pubsub not listening yet")

//ErrNeedToPointOutTopics 需要指名发布订阅器
var ErrNeedToPointOutTopics = errors.New("need to point out topics")
