package ranker

import "errors"

//ErrParamNMustBePositive 参数n必须是大于0
var ErrParamNMustBePositive = errors.New("参数n必须是大于0")

//ErrRankerror 排名不存在
var ErrRankerror = errors.New("排名不存在")
