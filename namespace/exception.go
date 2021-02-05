package namespace

import (
	"errors"
)

//ErrKeyNotHaveNamespace key没有命名空间
var ErrKeyNotHaveNamespace = errors.New("key没有命名空间")

//ErrParamDelimiterLengthMustLessThan2 不定长参数delimiter长度必须小于2
var ErrParamDelimiterLengthMustLessThan2 = errors.New("不定长参数delimiter长度必须小于2")

//ErrrParamDelimiterCannotEmpty 参数delimiter不能为空字符串
var ErrrParamDelimiterCannotEmpty = errors.New("参数delimiter不能为空字符串")

//ErrrParamEndpointCannotEmpty 参数endpoints的元素不能为空字符串
var ErrrParamEndpointCannotEmpty = errors.New("参数endpoints的元素不能为空字符串")

//ErrKeyParserNamespaceNumberNot2 key分隔出的元素不是2个
var ErrKeyParserNamespaceNumberNot2 = errors.New("key分隔出的元素不是2个")
