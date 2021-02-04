package estimatecount

import (
	"errors"
)

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrEstimateCountNotSetMaxTLL EstimateCount没有设置最大tll
var ErrEstimateCountNotSetMaxTLL = errors.New("EstimateCount没有设置最大tll")
