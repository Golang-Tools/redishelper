package bitmap

import (
	"errors"
)

//ErrParamScopLengthMoreThan2 不定长参数scop的长度不能大于2
var ErrParamScopLengthMoreThan2 = errors.New("不定长参数scop的长度不能大于2")
