package queuehelper

import (
	"errors"
)

//ErrQueueResNotTwo 从队列中得到的消息结果不为2位
var ErrQueueResNotTwo = errors.New("queue result not 2")

//ErrQueueAlreadyListened 队列已经被监听了
var ErrQueueAlreadyListened = errors.New("queue already listened")

//ErrQueueNotListeningYet 队列未被监听
var ErrQueueNotListeningYet = errors.New("queue not listening yet")
