// pubsubhelper 满足pchelper规定的生产者和消费者接口
package pubsubhelper

import (
	log "github.com/Golang-Tools/loggerhelper/v2"
)

var logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "redis-pubsubhelper"}))
	logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}
