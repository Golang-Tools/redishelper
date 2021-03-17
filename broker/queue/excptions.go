package queue

import (
	"errors"
)

//ErrQueueResNotTwo 从队列中得到的消息结果不为2位
var ErrQueueResNotTwo = errors.New("从队列中得到的消息结果不为2位")

//ErrQueueAlreadyListened 队列已经被监听了
var ErrQueueAlreadyListened = errors.New("队列已经被监听了")

//ErrQueueNotListeningYet 队列未被监听
var ErrQueueNotListeningYet = errors.New("队列未被监听")
