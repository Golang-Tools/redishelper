//Package redishelper 为代理添加Counter操作支持
//Counter可以用于分布式累加计数
package redishelper

import "context"

//Counter 加1后的当前计数值
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
func (proxy *redisHelper) Counter(ctx context.Context, key string) (int64, error) {
	if !proxy.IsOk() {
		return -1, ErrProxyNotYetSettedGoRedisV8Client
	}

	res, err := proxy.IncrBy(ctx, key, 1).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//CounterM 加m后的当前计数
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
//@params value int64 要增加的值.这个值可以为负
func (proxy *redisHelper) CounterM(ctx context.Context, key string, value int64) (int64, error) {
	if !proxy.IsOk() {
		return -1, ErrProxyNotYetSettedGoRedisV8Client
	}
	res, err := proxy.IncrBy(ctx, key, value).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//CounterReSet 重置当前计数器
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params key string 使用的键
func (proxy *redisHelper) CounterReSet(ctx context.Context, key string) error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedGoRedisV8Client
	}
	_, err := proxy.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	return nil
}
