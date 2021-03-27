package limiter

import (
	"errors"
)

//ErrLimiterMaxSizeMustLargerThanWaringSize limiter的最大容量必须大于警戒容量
var ErrLimiterMaxSizeMustLargerThanWaringSize = errors.New("limiter的最大容量必须大于警戒容量")
