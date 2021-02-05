package key

import (
	"errors"
)

//ErrKeyNotSetMaxTLL key没有设置最大tll
var ErrKeyNotSetMaxTLL = errors.New("key没有设置最大tll")

//ErrKeyNotExist key不存在
var ErrKeyNotExist = errors.New("key不存在")
