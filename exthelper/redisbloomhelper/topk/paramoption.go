package topk

import (
	"time"

	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/exthelper/redisbloomhelper"
	"github.com/Golang-Tools/redishelper/v2/middlewarehelper"
)

type InitOpts struct {
	Width       int64
	Depth       int64
	Decay       float64
	RefreshOpts []optparams.Option[middlewarehelper.RefreshOpt]
}

//InitWithWidth 使用概率方式初始化
func InitWithWidth(width int64) optparams.Option[InitOpts] {
	return optparams.NewFuncOption(func(o *InitOpts) {
		o.Width = width
	})
}

//InitWithDepth 使用概率方式初始化
func InitWithDepth(depth int64) optparams.Option[InitOpts] {
	return optparams.NewFuncOption(func(o *InitOpts) {
		o.Depth = depth
	})
}

//InitWithDecay 使用概率方式初始化
func InitWithDecay(decay float64) optparams.Option[InitOpts] {
	return optparams.NewFuncOption(func(o *InitOpts) {
		o.Decay = decay
	})
}

//mInit 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mInit(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[InitOpts] {
	return optparams.NewFuncOption(func(o *InitOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//InitWithhRefreshTTL 设置使用maxttl设置key的过期,pipeline无效
func InitWithRefreshTTL() optparams.Option[InitOpts] {
	return mInit(middlewarehelper.RefreshTTL())
}

//InitWithTTL 设置使用指定的ttl设置key的过期
func InitWithTTL(t time.Duration) optparams.Option[InitOpts] {
	return mInit(middlewarehelper.WithTTL(t))
}

type IncrOpts struct {
	RefreshOpts    []optparams.Option[middlewarehelper.RefreshOpt]
	Increment      int64    //incr专用
	MincrItems     []string //mincr专用
	MincrIncrement []int64  //mincr专用
}

//IncrWithIncrement IncrItem专用,为物品设置增量
func IncrWithIncrement(n int64) optparams.Option[IncrOpts] {
	return optparams.NewFuncOption(func(o *IncrOpts) {
		o.Increment = n
	})
}

//IncrWithItems MincrItem专用,设置items并且不添加增量,使用该参数标明将执行add而非incrby,因此不能和IncrWithItemMap以及IncrWithIncrItems混用
func IncrWithItems(items ...string) optparams.Option[IncrOpts] {
	return optparams.NewFuncOption(func(o *IncrOpts) {
		o.MincrItems = items
	})
}

//IncrWithItemMap MincrItem专用,使用字段形式设置items,使用该参数标明将执行incrby而非add,因此不能和IncrWithItems混用
// 注意增量小于1会被调整为1
func IncrWithItemMap(itemmap map[string]int64) optparams.Option[IncrOpts] {
	return optparams.NewFuncOption(func(o *IncrOpts) {
		if len(o.MincrItems) == 0 {
			o.MincrItems = []string{}
		}
		if len(o.MincrIncrement) == 0 {
			o.MincrIncrement = []int64{}
		}
		for item, increment := range itemmap {
			o.MincrItems = append(o.MincrItems, item)
			incr := increment
			if increment < 1 {
				incr = 1
			}
			o.MincrIncrement = append(o.MincrIncrement, incr)
		}
	})
}

//IncrWithIncrItems MincrItem专用,使用IncrItems的列表设置items,使用该参数标明将执行incrby而非add,因此不能和IncrWithItems混用
// 注意增量小于1会被调整为1
func IncrWithIncrItems(incritems ...*redisbloomhelper.IncrItem) optparams.Option[IncrOpts] {
	return optparams.NewFuncOption(func(o *IncrOpts) {
		if len(o.MincrItems) == 0 {
			o.MincrItems = []string{}
		}
		if len(o.MincrIncrement) == 0 {
			o.MincrIncrement = []int64{}
		}
		for _, incritem := range incritems {
			o.MincrItems = append(o.MincrItems, incritem.Item)
			incr := incritem.Increment
			if incritem.Increment < 1 {
				incr = 1
			}
			o.MincrIncrement = append(o.MincrIncrement, incr)
		}
	})
}

//mIncr 使用optparams.Option[middlewarehelper.Options]设置中间件属性
func mIncr(opts ...optparams.Option[middlewarehelper.RefreshOpt]) optparams.Option[IncrOpts] {
	return optparams.NewFuncOption(func(o *IncrOpts) {
		if o.RefreshOpts == nil {
			o.RefreshOpts = []optparams.Option[middlewarehelper.RefreshOpt]{}
		}
		o.RefreshOpts = append(o.RefreshOpts, opts...)
	})
}

//IncrWithhRefreshTTL 设置总是刷新
func IncrWithRefreshTTL() optparams.Option[IncrOpts] {
	return mIncr(middlewarehelper.RefreshTTL())
}

//IncrWithTTL 设置总是使用指定的ttl刷新key
func IncrWithTTL(t time.Duration) optparams.Option[IncrOpts] {
	return mIncr(middlewarehelper.WithTTL(t))
}
