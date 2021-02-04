package utils

import (
	"errors"
)

//ErrDefaultGeneratorAllreadySetted 默认的生成器已经被设置了
var ErrDefaultGeneratorAllreadySetted = errors.New("默认的生成器已经被设置了")

//ErrIndefiniteParameterClientLength 不定长参数clientID长度错误
var ErrIndefiniteParameterClientLength = errors.New("不定长参数clientID长度错误")

//ErrKeyNotSetMaxTLL key没有设置最大tll
var ErrKeyNotSetMaxTLL = errors.New("key没有设置最大tll")

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrDefaultGeneratorNotSetYet 默认的生成器未被设置
var ErrDefaultGeneratorNotSetYet = errors.New("默认的生成器未被设置")

//ErrQueueResNotTwo 从队列中得到的消息结果不为2位
var ErrQueueResNotTwo = errors.New("从队列中得到的消息结果不为2位")

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")

// // ErrStreamNotExist Stream不存在
// var ErrStreamNotExist = errors.New("Stream不存在")

//ErrStreamNeedToPointOutGroups 需要指名用户组
var ErrStreamNeedToPointOutGroups = errors.New("需要指名用户组")

//ErrStreamNeedToPointOutTopics 需要指名topic
var ErrStreamNeedToPointOutTopics = errors.New("需要指名topic")

//ErrStreamConsumerNotSetMaxTLL 流的消费者未设置生命周期
var ErrStreamConsumerNotSetMaxTLL = errors.New("流的消费者未设置生命周期")

//ErrStreamConsumerNotListeningYet 流客户端未监听
var ErrStreamConsumerNotListeningYet = errors.New("流客户端未开始监听流")

//ErrStreamConsumerAlreadyListened 流对象已经被监听了
var ErrStreamConsumerAlreadyListened = errors.New("流客户端已经在监听流")

//ErrParamMustBePositive 参数必须是大于0
var ErrParamMustBePositive = errors.New("参数必须是大于0")

//ErrElementNotExist 元素不存在
var ErrElementNotExist = errors.New("元素不存在")

//ErrRankerror 排名不存在
var ErrRankerror = errors.New("排名不存在")
