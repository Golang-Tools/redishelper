package message

import (
	"errors"
)

//ErrQueueResNotTwo 从队列中得到的消息结果不为2位
var ErrQueueResNotTwo = errors.New("从队列中得到的消息结果不为2位")
