//queuehelper 满足pchelper规定的生产者和消费者接口
package queuehelper

import (
	log "github.com/Golang-Tools/loggerhelper/v2"
)

var logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "redis-queuehelper"}))
	logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}
