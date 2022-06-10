//Package keyspace_notifications对象,用于管理监听redis事件,本工具实际是pubsub中sub的一个实例
package keyspace_notifications

import (
	"context"
	"errors"
	"strings"

	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/Golang-Tools/redishelper/v2/pchelper"
	"github.com/Golang-Tools/redishelper/v2/pubsubhelper"
	"github.com/go-redis/redis/v8"
)

//KeyspaceNotification 键空间通知的配置
type KeyspaceNotification struct {
	csm *pubsubhelper.Consumer
	opt Options
}

//New 创建一个键空间通知对象
func New(cli redis.UniversalClient, opts ...optparams.Option[Options]) (*KeyspaceNotification, error) {
	k := new(KeyspaceNotification)
	k.opt = Defaultopt
	optparams.GetOption(&k.opt, opts...)
	csm, err := pubsubhelper.NewConsumer(cli)
	if err != nil {
		return nil, err
	}
	k.csm = csm
	for topic, fn := range k.opt.NotificationEventHandlers {
		err := k.csm.RegistHandler(topic, fn)
		if err != nil {
			return nil, err
		}
	}
	return k, nil
}

//Start 监听事件
//@params opts ...optparams.Option[pchelper.ListenOptions] 监听时的一些配置,具体看listenoption.go说明
func (s *KeyspaceNotification) Start(opts ...optparams.Option[pchelper.ListenOptions]) error {
	_topics := []string{}
	for topic := range s.opt.NotificationEventHandlers {
		_topics = append(_topics, topic)
	}
	topics := strings.Join(_topics, ",")
	logger.Info("Start Listening", map[string]any{"topics": topics})
	return s.csm.Listen(topics, opts...)
}

//Stop 停止监听
func (s *KeyspaceNotification) Stop() error {
	return s.csm.StopListening()
}

//CheckConf 查看当前notify-keyspace-events的相关配置,具体可以查看<http://redisdoc.com/topic/notification.html>
func (c *KeyspaceNotification) CheckConf(ctx context.Context) (string, error) {
	res, err := c.csm.Client().ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return "", err
	}
	if len(res) != 2 {
		return "", errors.New("ConfigGet notify-keyspace-events not get 2 result")
	}
	return res[1].(string), nil
}

//SetConf 设置当前的notify-keyspace-events配置<http://redisdoc.com/topic/notification.html>
func (c *KeyspaceNotification) SetConf(ctx context.Context, conf string) error {
	res, err := c.csm.Client().ConfigSet(ctx, "notify-keyspace-events", conf).Result()
	if err != nil {
		return err
	}
	if strings.Contains(res, "OK") {
		return nil
	} else {
		return errors.New(res)
	}
}

var logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "redishelper-keyspace_notification"}))
	logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}
