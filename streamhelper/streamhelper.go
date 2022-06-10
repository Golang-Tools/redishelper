//streamhelper 满足pchelper规定的生产者和消费者接口,同时提供stream管理对象
package streamhelper

import (
	log "github.com/Golang-Tools/loggerhelper/v2"
)

var logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "redis-streamhelper"}))
	logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}
