package limiterhelper

import (
	"errors"
)

//ErrLimiterMaxSizeMustLargerThanWaringSize limiter的最大容量必须大于警戒容量
var ErrLimiterMaxSizeMustLargerThanWaringSize = errors.New("limiter's maxsize must larger than waring size")

//ErrAlreadyHasHook limiter的指定钩子已经设置过了
var ErrAlreadyHasHook = errors.New("already has hook")
