package clientkeybatch

import (
	"errors"
)

//ErrBatchNotSetMaxTLL batch没有设置最大tll
var ErrBatchNotSetMaxTLL = errors.New("batch没有设置最大tll")

//ErrKeysMustMoreThanOne keys参数个数必须大于0
var ErrKeysMustMoreThanOne = errors.New("keys参数个数必须大于0")

//ErrKeysMustSameClient keys必须使用相同的客户端对象
var ErrKeysMustSameClient = errors.New("keys必须使用相同的客户端对象")

//ErrKeysMustSameType keys必须为相同的类型
var ErrKeysMustSameType = errors.New("keys必须为相同的类型")
