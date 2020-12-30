package redishelper

//NameSpcaeKey 带命名空间的键
type NameSpcaeKey struct {
	namespace string
}

//NewNameSpaceKey 创建一个带命名空间的key
func NewNameSpaceKey(namespace string) *NameSpcaeKey {
	nsk := new(NameSpcaeKey)
	nsk.namespace = namespace
	return nsk
}

//AddSubNamespace 在原来的命名空间基础上加一级子命名空间创建一个新的命名空间
func (nsk *NameSpcaeKey) AddSubNamespace(namespace string) *NameSpcaeKey {
	nnsk := new(NameSpcaeKey)
	nnsk.namespace = nsk.namespace + "::" + namespace
	return nnsk
}

//Key 在命名空间基础上创建一个key
func (nsk *NameSpcaeKey) Key(key string) string {
	return nsk.namespace + "::" + key
}
