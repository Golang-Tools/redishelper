package redis_cell

//Options broker的配置
type Options struct {
	MaxBurst       int64
	CountPerPeriod int64
	Period         int64
}

var Defaultopt = Options{
	MaxBurst:       99,
	CountPerPeriod: 30,
	Period:         60,
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

type funcOption struct {
	f func(*Options)
}

func (fo *funcOption) Apply(do *Options) {
	fo.f(do)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

//WithMaxBurst 设置最大熔断数,最大token数为MaxBurst+1
func WithMaxBurst(maxBurst int64) Option {
	return newFuncOption(func(o *Options) {
		o.MaxBurst = maxBurst
	})
}

//WithCountPerPeriod 设置时间间隔内的token消减数
func WithCountPerPeriod(countPerPeriod int64) Option {
	return newFuncOption(func(o *Options) {
		o.CountPerPeriod = countPerPeriod
	})
}

//WithPeriod 设置token消减间隔时长,单位s
func WithPeriod(period int64) Option {
	return newFuncOption(func(o *Options) {
		o.Period = period
	})
}
