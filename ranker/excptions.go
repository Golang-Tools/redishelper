package ranker

import (
	"errors"
)

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")

//ErrCountMustBePositive 参数必须是大于0
var ErrCountMustBePositive = errors.New("参数必须是大于0")

//ErrElementNotExist 元素不存在
var ErrElementNotExist = errors.New("元素不存在")

//ErrRankerror 排名不存在
var ErrRankerror = errors.New("排名不存在")

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrRankerNotSetMaxTLL Ranker没有设置最大tll
var ErrRankerNotSetMaxTLL = errors.New("Ranker没有设置最大tll")
