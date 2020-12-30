package redishelper

// distributedcounter 分布式计数器
type distributedcounter struct {
	proxy *redisHelper
	key   string
}

func newCounter(proxy *redisHelper, key string) *distributedcounter {
	counter := new(distributedcounter)
	counter.key = key
	counter.proxy = proxy
	return counter
}

// Count 加1后的当前计数
func (counter *distributedcounter) Count() (int64, error) {
	if !counter.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := counter.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	res, err := conn.IncrBy(counter.key, 1).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

// Count 加1后的当前计数
func (counter *distributedcounter) CountM(value int64) (int64, error) {
	if !counter.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := counter.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	res, err := conn.IncrBy(counter.key, value).Result()
	if err != nil {
		return 0, err
	}
	return res, nil
}

//ReSet 重置当前计数器
func (counter *distributedcounter) ReSet() error {
	if !counter.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := counter.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.Del(counter.key).Result()
	if err != nil {
		return err
	}
	return nil
}
