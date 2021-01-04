package queue

import (
	"errors"
)

//ErrIndefiniteParameterLength 不定长参数长度不匹配
var ErrIndefiniteParameterLength = errors.New("不定长参数长度错误")

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")

//ErrBitmapNotSetMaxTLL bitmap没有设置最大tll
var ErrBitmapNotSetMaxTLL = errors.New("kbitmap没有设置最大tll")
