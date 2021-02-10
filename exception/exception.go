package exception

import (
	"errors"
)

//ErrParamOptsLengthMustLessThan2 不定长参数opts长度必须小于2
var ErrParamOptsLengthMustLessThan2 = errors.New("不定长参数opts长度必须小于2")

//ErrParamScopLengthMoreThan2 不定长参数scop的长度不能大于2
var ErrParamScopLengthMoreThan2 = errors.New("不定长参数scop的长度不能大于2")
