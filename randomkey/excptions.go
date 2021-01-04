package randomkey

import (
	"errors"
)

//ErrDefaultGeneratorAllreadySetted 默认的生成器已经被设置了
var ErrDefaultGeneratorAllreadySetted = errors.New("默认的生成器已经被设置了")

//ErrIndefiniteParameterClientLength 不定长参数clientID长度错误
var ErrIndefiniteParameterClientLength = errors.New("不定长参数clientID长度错误")

//ErrDefaultGeneratorNotSetYet 默认的生成器未被设置
var ErrDefaultGeneratorNotSetYet = errors.New("默认的生成器未被设置")
