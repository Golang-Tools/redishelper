package redisbloomhelper

import (
	"errors"
)

//ErrItemListEmpty  物品列表为空
var ErrItemListEmpty = errors.New("item list is empty")

//ErrMustChooseOneParam 必须选一个参数填入
var ErrMustChooseOneParam = errors.New("must choose one param")

//ErrItemLenNotMatch  物品数量不符
var ErrItemLenNotMatch = errors.New("item len not match")

//ErrResultLenNotMatch  结果返回数量不符
var ErrResultLenNotMatch = errors.New("results len not match")

//ErrNeedMoreThan2Source  需要超过2个资源
var ErrNeedMoreThan2Source = errors.New("need more than 2 source")

//ErrEmptyValue 返回值为空值
var ErrEmptyValue = errors.New("empty value")
