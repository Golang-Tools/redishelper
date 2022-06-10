package ranker

import "errors"

//ErrRankerror 排名不存在
var ErrRankerror = errors.New("ranker ror")

//ErrKeyNotExists key不存在
var ErrKeyNotExists = errors.New("key not exists")

//ErrElementNotExist 元素不存在
var ErrElementNotExist = errors.New("element not exist")

//ErrParamNMustBePositive 参数n必须是大于0
var ErrParamNMustBePositive = errors.New("param n must positive")
