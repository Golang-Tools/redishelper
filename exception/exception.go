package exception

import (
	"errors"
)

//ErrParamOptsLengthMustLessThan2 不定长参数opts长度必须小于2
var ErrParamOptsLengthMustLessThan2 = errors.New("不定长参数opts长度必须小于2")
