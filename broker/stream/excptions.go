package stream

import (
	"errors"
)

//ErrStreamNeedToPointOutGroups 流操作需要指定消费者组
var ErrStreamNeedToPointOutGroups = errors.New("stream操作需要指定消费者组")

//ErrStreamConsumerAlreadyListened 流已经被监听了
var ErrStreamConsumerAlreadyListened = errors.New("流已经被监听了")

//ErrStreamConsumerNotListeningYet 流未被监听
var ErrStreamConsumerNotListeningYet = errors.New("流未被监听")
