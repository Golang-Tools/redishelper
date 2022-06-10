package streamhelper

import (
	"errors"
)

//ErrStreamNeedToPointOutGroups 流操作需要指定消费者组
var ErrStreamNeedToPointOutGroups = errors.New("stream need to point out groups")

//ErrStreamConsumerAlreadyListened 流已经被监听了
var ErrStreamConsumerAlreadyListened = errors.New("stream already listened")

//ErrStreamConsumerNotListeningYet 流未被监听
var ErrStreamConsumerNotListeningYet = errors.New("stream not listening yet")
