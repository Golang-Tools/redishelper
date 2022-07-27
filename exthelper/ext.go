//exthelper 用于管理redis中扩展的帮助模块
package exthelper

import (
	"context"
	"errors"

	// log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/go-redis/redis/v8"
)

type Module struct {
	Name    string
	Version int64
}

//ListModule 查看redis服务端加载的扩展模块
func ListModule(client redis.UniversalClient, ctx context.Context) ([]*Module, error) {
	moduleinfos, err := client.Do(ctx, "module", "list").Result()
	if err != nil {
		return nil, err
	}
	// log.Info("get moduleinfos", log.Dict{"moduleinfos": moduleinfos})
	infos, ok := moduleinfos.([]interface{})
	if !ok {
		return nil, errors.New("cannot parser moduleinfos to []interface{}")
	}
	if len(infos) > 0 {
		result := []*Module{}
		for _, infoi := range infos {
			info, ok := infoi.([]interface{})
			if !ok {
				return nil, errors.New("cannot parser moduleinfo to []interface{}")
			}
			result = append(result, &Module{
				Name:    info[1].(string),
				Version: info[3].(int64),
			})
		}
		return result, nil
	}
	return []*Module{}, nil
}

//CheckModule 检查模块是否被加载
func CheckModule(client redis.UniversalClient, ctx context.Context, modulename string) (*Module, error) {
	ms, err := ListModule(client, ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range ms {
		if m.Name == modulename {
			return m, nil
		}
	}
	return nil, nil
}
