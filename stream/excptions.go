package stream

//异常
import (
	"errors"
)

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")

// ErrStreamNotExist Stream不存在
var ErrStreamNotExist = errors.New("Stream不存在")

//ErrStreamNotSetMaxTLL bitmap没有设置最大tll
var ErrStreamNotSetMaxTLL = errors.New("stream没有设置最大tll")

//ErrNeedToPointOutGroups 需要指名用户组
var ErrNeedToPointOutGroups = errors.New("需要指名用户组")

//ErrNeedToPointOutTopics 需要指名流
var ErrNeedToPointOutTopics = errors.New("需要指名流")

//ErrConsumerNotSetMaxTLL 流的消费者未设置生命周期
var ErrConsumerNotSetMaxTLL = errors.New("流的消费者未设置生命周期")

//ErrConsumerNotListeningYet 队列未被监听
var ErrConsumerNotListeningYet = errors.New("队列未被监听")

//ErrConsumerAlreadyListened 流对象已经被监听了
var ErrConsumerAlreadyListened = errors.New("流对象已经被监听了")
