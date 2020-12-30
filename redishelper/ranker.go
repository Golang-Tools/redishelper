package redishelper

import (
	"fmt"

	"github.com/go-redis/redis/v7"
)

type ranker struct {
	proxy *redisHelper
	Name  string
}

//newRanker 新建一个排序器
func newRanker(proxy *redisHelper, name string) *ranker {
	s := new(ranker)
	s.Name = name
	s.proxy = proxy
	return s
}

// Len 获取排名器的当前长度
func (r *ranker) Len() (int64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.ZCard(r.Name).Result()
}

// Add 增加一个新元素
func (r *ranker) Add(elements ...*redis.Z) (int64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.ZAddNX(r.Name, elements...).Result()
}

// Update 更新元素的权重
func (r *ranker) Update(elements ...*redis.Z) (int64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.ZAddXX(r.Name, elements...).Result()
}

// AddOrUpdate 如果元素存在则更新元素权重,不存在则增加元素
func (r *ranker) AddOrUpdate(elements ...*redis.Z) (int64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.ZAdd(r.Name, elements...).Result()
}

// Remove 删除元素
func (r *ranker) Remove(elements ...interface{}) (int64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.ZRem(r.Name, elements...).Result()
}

// INCR 为元素累增权重,如果不设置权重则默认累增1
func (r *ranker) INCR(element string, weight ...float64) (float64, error) {
	if !r.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	var w float64
	if len(weight) == 1 {
		w = weight[0]
	} else {
		w = float64(1)
	}
	return conn.ZIncrBy(r.Name, w, element).Result()
}

//Range 获取排名范围内的元素,desc为True则为从大到小否则为从小到大
func (r *ranker) Range(start, end int64, desc bool) ([]string, error) {
	if !r.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	if desc {
		return conn.ZRevRange(r.Name, start, end).Result()
	}
	return conn.ZRange(r.Name, start, end).Result()
}

//First 获取排名前若干位的元素,desc为True则为从大到小否则为从小到大
func (r *ranker) First(count int64, desc bool) ([]string, error) {
	if count <= 0 {
		return nil, ErrCountMustBePositive
	}
	return r.Range(int64(0), count-1, desc)
}
func (r *ranker) Head(count int64, desc bool) ([]string, error) {
	return r.First(count, desc)
}

//Last 获取排名后若干位的元素,desc为True则为从大到小否则为从小到大
func (r *ranker) Last(count int64, desc bool) ([]string, error) {
	if count <= 0 {
		return nil, ErrCountMustBePositive
	}
	d := !desc
	return r.Range(int64(0), count-1, d)
}
func (r *ranker) Tail(count int64, desc bool) ([]string, error) {
	return r.Last(count, desc)
}

//GetRank 获取指定元素的排名,desc为True则为从大到小否则为从小到大
func (r *ranker) GetRank(element string, desc bool) (int64, error) {
	if !r.proxy.IsOk() {
		return -1, ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return -1, err
	}
	if desc {
		res, err := conn.ZRevRank(r.Name, element).Result()
		fmt.Println("############@")
		fmt.Println(res)
		fmt.Println(err)
		fmt.Println("############@")
		if err != nil {
			if err == redis.Nil {
				return -1, ErrElementNotExist
			}
			return -1, err
		}
		return res + 1, nil
	}
	res, err := conn.ZRank(r.Name, element).Result()
	if err != nil {
		if err == redis.Nil {
			return -1, ErrElementNotExist
		}
		return -1, err
	}
	return res + 1, nil

}

//GetElementByRank 获取指定排名的元素,desc为True则为从大到小否则为从小到大
func (r *ranker) GetElementByRank(rank int64, desc bool) (string, error) {
	res, err := r.Range(rank, rank, desc)
	if err != nil {
		return "", err
	}
	if len(res) != 1 {
		return "", ErrRankError
	}
	return res[0], nil
}

//Close 关闭排名器
func (r *ranker) Close() error {
	if !r.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := r.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.Del(r.Name).Result()
	if err != nil {
		return err
	}
	return nil
}
