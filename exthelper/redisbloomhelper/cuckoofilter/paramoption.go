package cuckoofilter

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
)

//AddOpt filter添加元素物品的参数
type AddOpts struct {
	NX          bool
	RefreshOpts []optparams.Option[middlewarehelper.RefreshOpt]
}

//AddWithoutNX 设置cuckoofilter中只存在不存在都会添加
func AddWithoutNX() optparams.Option[AddOpts] {
	return optparams.NewFuncOption(func(o *AddOpts) {
		o.NX = false
	})
}

//mAdd 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mAdd(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[AddOpts] {
	return optparams.NewFuncOption(func(o *AddOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//AddWithhRefreshTTL 设置总是刷新,pipeline中无效
func AddWithRefreshTTL() optparams.Option[AddOpts] {
	return mAdd(middlewarehelper.RefreshTTL())
}

//AddWithTTL 设置总是使用指定的ttl刷新key
func AddWithTTL(t time.Duration) optparams.Option[AddOpts] {
	return mAdd(middlewarehelper.WithTTL(t))
}

//AddWithRefreshTTLAtFirstTime 设置第一次创建key时使用MaxTTL设置过期,pipeline中无效
func AddWithRefreshTTLAtFirstTime() optparams.Option[AddOpts] {
	return mAdd(middlewarehelper.RefreshTTLAtFirstTime())
}

//AddWithTTLAtFirstTime 设置第一次创建key时使用指定的ttl设置过期,pipeline中无效
func AddWithTTLAtFirstTime(t time.Duration) optparams.Option[AddOpts] {
	return mAdd(middlewarehelper.WithTTLAtFirstTime(t))
}

type ReserveOpts struct {
	Expansion     int64
	BucketSize    int64
	MaxIterations int64
	RefreshOpts   []optparams.Option[middlewarehelper.RefreshOpt]
}

//ReserveWithExpansion 当达到容量时进行扩容.
// bloomfilter会创建一个额外的子过滤器,新子过滤器的大小是最后一个子过滤器的大小乘以扩展设置的值.
// 如果要存储在过滤器中的元素数量未知我们建议您使用`2`或更多的扩展来减少子过滤器的数量;
// 否则我们建议您使用1的扩展来减少内存消耗.默认扩展值为 2
// cuckoofilter则会扩容到2^n
func ReserveWithExpansion(expansion int64) optparams.Option[ReserveOpts] {
	return optparams.NewFuncOption(func(o *ReserveOpts) {
		o.Expansion = expansion
	})
}

//ReserveWithBucketsize 每个存储桶中的项目数.较高的桶大小值会提高填充率,但也会导致更高的错误率和稍慢的性能.
func ReserveWithBucketSize(bucketsize int64) optparams.Option[ReserveOpts] {
	return optparams.NewFuncOption(func(o *ReserveOpts) {
		o.BucketSize = bucketsize
	})
}

//ReserveWithMaxIterations 在声明过滤器已满并创建附加过滤器之前尝试在存储桶之间交换项目的次数.较低的值对性能更好,较高的值对过滤器填充率更好.
func ReserveWithMaxIterations(maxiterations int64) optparams.Option[ReserveOpts] {
	return optparams.NewFuncOption(func(o *ReserveOpts) {
		o.MaxIterations = maxiterations
	})
}

//mReserve 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mReserve(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[ReserveOpts] {
	return optparams.NewFuncOption(func(o *ReserveOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//ReserveWithhRefreshTTL 设置使用maxttl设置key的过期,pipeline无效
func ReserveWithRefreshTTL() optparams.Option[ReserveOpts] {
	return mReserve(middlewarehelper.RefreshTTL())
}

//ReserveWithTTL 设置使用指定的ttl设置key的过期
func ReserveWithTTL(t time.Duration) optparams.Option[ReserveOpts] {
	return mReserve(middlewarehelper.WithTTL(t))
}

type InsertOpts struct {
	NoCreate    bool
	Capacity    int64
	Expansion   int64
	NX          bool
	RefreshOpts []optparams.Option[middlewarehelper.RefreshOpt]
}

//InsertWithNoCreate 如果过滤器不存在则不创建它.这可以用于过滤器创建和过滤器添加之间需要严格分离的地方.`NOCREATE`的优先级高于`CAPACITY`和`ERROR`
func InsertWithNoCreate() optparams.Option[InsertOpts] {
	return optparams.NewFuncOption(func(o *InsertOpts) {
		o.NoCreate = true
	})
}

//InsertWithCapacity 容量,预估物品的数量.当过滤器已经存在时用于创建过滤器.容量越大检索效率越低,但如果超出容量则会默认使用子过滤器扩容,这对检索效率的影响更大.当`NOCREATE`存在时这个设置将失效
func InsertWithCapacity(capacity int64) optparams.Option[InsertOpts] {
	return optparams.NewFuncOption(func(o *InsertOpts) {
		o.Capacity = capacity
	})
}

//InsertWithExpansion 当达到容量时进行扩容.
// 使用范围: bloomfilter
// bloomfilter会创建一个额外的子过滤器,新子过滤器的大小是最后一个子过滤器的大小乘以扩展设置的值.
// 如果要存储在过滤器中的元素数量未知我们建议您使用`2`或更多的扩展来减少子过滤器的数量;
// 否则我们建议您使用1的扩展来减少内存消耗.默认扩展值为 2
// cuckoofilter则会扩容到2^n
func InsertWithExpansion(expansion int64) optparams.Option[InsertOpts] {
	return optparams.NewFuncOption(func(o *InsertOpts) {
		o.Expansion = expansion
	})
}

//InsertWithoutNX 设置cuckoofilter中无论存在不存在物品都进行插入
func InsertWithoutNX() optparams.Option[InsertOpts] {
	return optparams.NewFuncOption(func(o *InsertOpts) {
		o.NX = false
	})
}

//mInsert 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mInsert(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[InsertOpts] {
	return optparams.NewFuncOption(func(o *InsertOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//InsertWithhRefreshTTL 设置总是刷新
func InsertWithRefreshTTL() optparams.Option[InsertOpts] {
	return mInsert(middlewarehelper.RefreshTTL())
}

//InsertWithTTL 设置总是使用指定的ttl刷新key
func InsertWithTTL(t time.Duration) optparams.Option[InsertOpts] {
	return mInsert(middlewarehelper.WithTTL(t))
}

//InsertWithRefreshTTLAtFirstTime 设置第一次创建key时使用MaxTTL设置过期
func InsertWithRefreshTTLAtFirstTime() optparams.Option[InsertOpts] {
	return mInsert(middlewarehelper.RefreshTTLAtFirstTime())
}

//InsertWithTTLAtFirstTime 设置第一次创建key时使用指定的ttl设置过期
func InsertWithTTLAtFirstTime(t time.Duration) optparams.Option[InsertOpts] {
	return mInsert(middlewarehelper.WithTTLAtFirstTime(t))
}
