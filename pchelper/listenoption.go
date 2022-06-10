package pchelper

import (
	"strconv"
	"time"

	"github.com/Golang-Tools/optparams"
)

//ListenOptions 消费端listen方法的配置
type ListenOptions struct {
	ParallelHanddler bool
	Parser           EventParser
	TopicStarts      map[string]string //用于指定特定topic的监听起始位置
}

var DefaultListenOpt = ListenOptions{
	Parser:      DefaultParser,
	TopicStarts: map[string]string{},
}

//WithParallelHanddler 并行执行注册的handdler
func WithParallelHanddler() optparams.Option[ListenOptions] {
	return optparams.NewFuncOption(func(o *ListenOptions) {
		o.ParallelHanddler = true
	})
}

//WithParallelHanddler 并行执行注册的handdler
func WithEventParser(fn EventParser) optparams.Option[ListenOptions] {
	return optparams.NewFuncOption(func(o *ListenOptions) {
		o.Parser = fn
	})
}

//WithTopicStartAt stream消费者专用,用于设定指定topic消费起始时间
func WithTopicStartAt(topic string, t time.Time) optparams.Option[ListenOptions] {
	return optparams.NewFuncOption(func(o *ListenOptions) {
		n := t.UnixNano()
		ns := strconv.FormatInt(n, 10)
		if o.TopicStarts == nil {
			o.TopicStarts = map[string]string{}
		}
		o.TopicStarts[topic] = ns
	})
}

//WithTopicStartPosition stream消费者专用,用于设定指定topic消费起始位置
func WithTopicStartPosition(topic, flag string) optparams.Option[ListenOptions] {
	return optparams.NewFuncOption(func(o *ListenOptions) {
		if o.TopicStarts == nil {
			o.TopicStarts = map[string]string{}
		}
		o.TopicStarts[topic] = flag
	})
}

//WithTopicsStartPositionMap stream消费者专用,用于设定指定复数topic消费起始位置
func WithTopicsStartPositionMap(setting map[string]string) optparams.Option[ListenOptions] {
	return optparams.NewFuncOption(func(o *ListenOptions) {
		if o.TopicStarts == nil {
			o.TopicStarts = map[string]string{}
		}
		for topic, flag := range setting {
			o.TopicStarts[topic] = flag
		}
	})
}
