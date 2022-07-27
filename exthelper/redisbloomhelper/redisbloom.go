package redisbloomhelper

import (
	"fmt"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/go-redis/redis/v8"
)

type IncrItem struct {
	Item      string `json:"Item"`
	Increment int64  `json:"Increment"`
}

//BoolValPlacehold 将结果中的1设置为true,0设置为false;如果Reverse为true则相反
type BoolValPlacehold struct {
	Ck      string
	Cmd     *redis.Cmd
	Reverse bool
}

func (r *BoolValPlacehold) Result() (bool, error) {
	res := r.Cmd.Val()
	if res == nil {
		return false, ErrEmptyValue
	}
	existsint, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("cannot parser %s results to int64", r.Ck)
	}
	var result bool
	if existsint == 1 {
		result = true
	} else {
		result = false
	}
	if r.Reverse {
		return !result, nil
	}
	return result, nil
}

//BoolMapPlacehold 将结果中的1设置为true,0设置为false;如果Reverse为true则相反
type BoolMapPlacehold struct {
	Ck      string
	Cmd     *redis.Cmd
	Items   []string
	Reverse bool
}

func (r *BoolMapPlacehold) Result() (map[string]bool, error) {
	res := r.Cmd.Val()
	if res == nil {
		return nil, ErrEmptyValue
	}
	infos, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", r.Ck)
	}
	if len(infos) != len(r.Items) {
		log.Info("ErrResultLenNotMatch", log.Dict{"infos": infos, "r.Items": r.Items, "ck": r.Ck})
		return nil, ErrResultLenNotMatch
	}
	result := map[string]bool{}
	for index, item := range r.Items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, fmt.Errorf("cannot parser %s result to int64", r.Ck)
		}
		if existsint == 1 {
			result[item] = true
		} else {
			result[item] = false
		}
	}
	if r.Reverse {
		for i, f := range result {
			result[i] = !f
		}
	}

	return result, nil
}

// OneBoolMapPlacehold bool结果和对应物品映射结果但只取其中一个值的占位符
type OneBoolMapPlacehold struct {
	MapPlacehold *BoolMapPlacehold
	Item         string
}

func (r *OneBoolMapPlacehold) Result() (bool, error) {
	resmap, err := r.MapPlacehold.Result()
	if err != nil {
		return false, err
	}
	res, ok := resmap[r.Item]
	if !ok {
		return false, fmt.Errorf("item %s not found in result map", r.Item)
	}
	return res, nil
}

//Int64ValPlacehold int64型的结果占位符
type Int64ValPlacehold struct {
	Ck  string
	Cmd *redis.Cmd
}

func (r *Int64ValPlacehold) Result() (int64, error) {
	res := r.Cmd.Val()
	if res == nil {
		return 0, ErrEmptyValue
	}
	existsint, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("cannot parser %s results to int64", r.Ck)
	}
	return existsint, nil
}

//Int64MapPlacehold int64结果和对应物品映射结果的占位符
type Int64MapPlacehold struct {
	Ck    string
	Cmd   *redis.Cmd
	Items []string
}

func (r *Int64MapPlacehold) Result() (map[string]int64, error) {
	res := r.Cmd.Val()
	if res == nil {
		return nil, ErrEmptyValue
	}
	infos, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", r.Ck)
	}
	if len(infos) != len(r.Items) {
		log.Info("ErrResultLenNotMatch", log.Dict{"infos": infos, "r.Items": r.Items, "ck": r.Ck})
		return nil, ErrResultLenNotMatch
	}
	result := map[string]int64{}
	for index, item := range r.Items {
		existsint, ok := infos[index].(int64)
		if !ok {
			return nil, fmt.Errorf("cannot parser %s result to int64", r.Ck)
		}
		result[item] = existsint
	}
	return result, nil
}

// OneInt64MapPlacehold int64结果和对应物品映射结果但只取其中一个值的占位符
type OneInt64MapPlacehold struct {
	MapPlacehold *Int64MapPlacehold
	Item         string
}

func (r *OneInt64MapPlacehold) Result() (int64, error) {
	resmap, err := r.MapPlacehold.Result()
	if err != nil {
		return 0, err
	}
	res, ok := resmap[r.Item]
	if !ok {
		return 0, fmt.Errorf("item %s not found in result map", r.Item)
	}
	return res, nil
}

//StringListValPlacehold []string型的结果占位符
type StringListValPlacehold struct {
	Ck  string
	Cmd *redis.Cmd
}

func (r *StringListValPlacehold) Result() ([]string, error) {
	res := r.Cmd.Val()
	if res == nil {
		return nil, ErrEmptyValue
	}
	infos, ok := res.([]interface{})

	if !ok {
		return nil, fmt.Errorf("cannot parser %s results to []interface{}", r.Ck)
	}
	result := []string{}
	for _, expelleditem := range infos {
		if expelleditem != nil {
			item, ok := expelleditem.(string)
			if !ok {
				return nil, fmt.Errorf("cannot parser %s result to string", r.Ck)
			}
			result = append(result, item)
		}
	}
	return result, nil
}
