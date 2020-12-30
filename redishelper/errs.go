package redishelper

import (
	"errors"
)

// ErrHelperNotInited 代理未初始化错误
var ErrHelperNotInited = errors.New("proxy not inited yet")

// ErrHelperAlreadyInited 代理已经初始化错误
var ErrHelperAlreadyInited = errors.New("proxy already inited yet")

// ErrLockAlreadySet 分布式锁已经设置
var ErrLockAlreadySet = errors.New("Lock is already setted")

//ErrLockWaitTimeout 等待解锁超时
var ErrLockWaitTimeout = errors.New("wait lock timeout")

//ErrKeyNotExist 键不存在
var ErrKeyNotExist = errors.New("key not exist")

//ErrGroupNotInTopic 消费组不在topic上
var ErrGroupNotInTopic = errors.New("Group Not In Topic")

//ErrStreamConsumerNotBlocked 消费者不是用阻塞模式监听
var ErrStreamConsumerNotBlocked = errors.New("Stream Consumer Must Be Blocked")

//ErrPubSubNotSubscribe Pubsub模式订阅者未订阅
var ErrPubSubNotSubscribe = errors.New("PubSub Not Subscribe")

//ErrQueueGetNotMatch Pubsub模式订阅者未订阅
var ErrQueueGetNotMatch = errors.New("Queue Get NotMatch")

//ErrIsAlreadySubscribed 消费者已经订阅了数据
var ErrIsAlreadySubscribed = errors.New("Is Already Subscribed")

//ErrIsAlreadyUnSubscribed 消费者已经取消订阅了数据
var ErrIsAlreadyUnSubscribed = errors.New("Is Already UnSubscribed")

//ErrRankError 排名信息有问题
var ErrRankError = errors.New("Rank Error")

//ErrCountMustBePositive 个数参数必须为正数
var ErrCountMustBePositive = errors.New("Count Must Be Positive")

//ErrElementNotExist 个数参数必须为正数
var ErrElementNotExist = errors.New("Element Not Exist")

// Done 锁的结束信号
var Done = errors.New("done")
