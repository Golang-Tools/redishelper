package redishelper

import (
	"context"
	"fmt"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/go-redis/redis/v7"
)

//StreamTopic 流主题
type StreamTopic struct {
	proxy  *redisHelper
	Name   string
	MaxLen int64
	Strict bool //maxlen with ~
}

//NewStreamTopic 新建一个流主题
func NewStreamTopic(proxy *redisHelper, name string, maxlen int64, strict bool) *StreamTopic {
	s := new(StreamTopic)
	s.Name = name
	s.proxy = proxy
	s.MaxLen = maxlen
	s.Strict = strict
	return s
}

//Len 查看主题流的长度
func (topic *StreamTopic) Len() (int64, error) {
	if !topic.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.XLen(topic.Name).Result()
}

//Trim 为主题扩容
func (topic *StreamTopic) Trim(count int64) (int64, error) {
	if !topic.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	return conn.XTrim(topic.Name, count).Result()
}

//Delete 设置标志位标识删除主题流中指定id的数据
func (topic *StreamTopic) Delete(ids ...string) error {
	if !topic.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return err
	}
	_, err = conn.XDel(topic.Name, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Range 获取消息列表,会自动过滤已经删除的消息,注意-表示最小值, +表示最大值
func (topic *StreamTopic) Range(start, stop string) ([]redis.XMessage, error) {
	if !topic.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return conn.XRange(topic.Name, start, stop).Result()
}

//Publish 向主题流发送消息
func (topic *StreamTopic) Publish(value map[string]interface{}) (string, error) {
	if !topic.proxy.IsOk() {
		return "", ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return "", err
	}
	args := redis.XAddArgs{
		Stream: topic.Name,
		Values: value,
		ID:     "*",
	}
	if topic.MaxLen != 0 {
		if topic.Strict {
			args.MaxLen = topic.MaxLen
		} else {
			args.MaxLenApprox = topic.MaxLen
		}
	}

	res, err := conn.XAdd(&args).Result()
	if err != nil {
		fmt.Println("publish error:", err.Error())
	}

	return res, err
}

//GroupInfos 获取主题流中注册的消费者组信息
func (topic *StreamTopic) GroupInfos() ([]redis.XInfoGroups, error) {
	if !topic.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return conn.XInfoGroups(topic.Name).Result()
}

//HasGroup 判断消费者组是否在topic上
func (topic *StreamTopic) HasGroup(groupname string) (bool, error) {
	groups, err := topic.GroupInfos()
	if err != nil {
		return false, err
	}
	for _, group := range groups {
		if group.Name == groupname {
			return true, nil
		}
	}
	return false, nil
}

//HasGroups 判断消费者组是否都在在topic上
func (topic *StreamTopic) HasGroups(groupnames []string) (bool, error) {
	groups, err := topic.GroupInfos()
	if err != nil {
		return false, err
	}
	groupnamesSet := set.NewSet()
	hasgroupSet := set.NewSet()
	for _, name := range groupnames {
		groupnamesSet.Add(name)
	}

	for _, group := range groups {
		hasgroupSet.Add(group.Name)
	}
	hasgroupSet.IsSuperset(groupnamesSet)
	return hasgroupSet.IsSuperset(groupnamesSet), nil
}

//CreateGroup 为指定消费者在指定的topic上创建消费者组
func (topic *StreamTopic) CreateGroup(groupname, start string) (string, error) {
	if !topic.proxy.IsOk() {
		return "", ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return "", err
	}
	return conn.XGroupCreate(topic.Name, groupname, start).Result()
}

//DeleteGroup 为指定消费者在指定的topic上删除消费者组
func (topic *StreamTopic) DeleteGroup(groupname string) (int64, error) {
	if !topic.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	has, err := topic.HasGroup(groupname)
	if err != nil {
		return 0, err
	}
	if has {
		return conn.XGroupDestroy(topic.Name, groupname).Result()
	}
	return 0, ErrGroupNotInTopic
}

//DeleteConsumer 在指定的topic上删除指定消费者组中的指定消费者
func (topic *StreamTopic) DeleteConsumer(groupname string, consumername string) (int64, error) {
	if !topic.proxy.IsOk() {
		return 0, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return 0, err
	}
	has, err := topic.HasGroup(groupname)
	if err != nil {
		return 0, err
	}
	if has {
		return conn.XGroupDelConsumer(topic.Name, groupname, consumername).Result()
	}
	return 0, ErrGroupNotInTopic
}

//SetGroupID 设置指定消费者组在主题流中的读取起始位置
func (topic *StreamTopic) SetGroupID(groupname string, start string) (string, error) {
	if !topic.proxy.IsOk() {
		return "", ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return "", err
	}
	has, err := topic.HasGroup(groupname)
	if err != nil {
		return "", err
	}
	if has {
		return conn.XGroupSetID(topic.Name, groupname, start).Result()
	}
	return "", ErrGroupNotInTopic
}

//Pending 查看消费组中等待确认的消息列表
func (topic *StreamTopic) Pending(groupname string) (*redis.XPending, error) {
	if !topic.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	has, err := topic.HasGroup(groupname)
	if err != nil {
		return nil, err
	}
	if has {
		return conn.XPending(topic.Name, groupname).Result()
	}
	return nil, ErrGroupNotInTopic
}

//Move 查看消费组中等待确认的消息列表
func (topic *StreamTopic) Move(groupname string, toconsumer string, minIdle time.Duration, ids ...string) ([]redis.XMessage, error) {
	if !topic.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := topic.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	has, err := topic.HasGroup(groupname)
	if err != nil {
		return nil, err
	}
	if has {
		args := redis.XClaimArgs{
			Stream:   topic.Name,
			Group:    groupname,
			MinIdle:  minIdle,
			Messages: ids,
		}
		return conn.XClaim(&args).Result()
	}
	return nil, ErrGroupNotInTopic
}

//streamProducer 流对象
type streamProducer struct {
	Topic *StreamTopic
}

func newStreamProducerFromStreamTopic(topic *StreamTopic) *streamProducer {
	s := new(streamProducer)
	s.Topic = topic
	return s
}

func newStreamProducer(proxy *redisHelper, topic string, maxlen int64, strict bool) *streamProducer {
	t := NewStreamTopic(proxy, topic, maxlen, strict)
	s := newStreamProducerFromStreamTopic(t)
	return s
}

//Publish 向流发送消息
func (producer *streamProducer) Publish(value map[string]interface{}) (string, error) {
	return producer.Topic.Publish(value)
}

type streamConsumer struct {
	stopch        chan error
	isSubscribed  bool
	proxy         *redisHelper  //使用的redis连接代理
	Topics        []string      //监听的topic
	Count         int64         //一次读取多少条消息
	Block         time.Duration //若有设置,阻塞等待消息,等待超时为设置的值
	Start         string        //从什么位置开始监听
	ConsumerGroup string        //监听使用的消费组,仅对消费组形式有效
	ConsumerName  string        //监听使用的消费者名仅对消费组形式有效
	NoAck         bool          //是否不确认消息收到与否,仅对消费组形式有效
}

func newStreamConsumer(proxy *redisHelper, topics []string, start string, count int64, block int64, name string, noack bool, group ...string) *streamConsumer {
	s := new(streamConsumer)
	s.stopch = make(chan error)
	s.isSubscribed = false
	s.Topics = topics
	s.proxy = proxy
	s.Start = start
	s.NoAck = noack
	if count != 0 {
		s.Count = count
	}
	if block != 0 {
		s.Block = time.Duration(block) * time.Second
	}
	if name != "" {
		s.ConsumerName = name
	}
	switch len(group) {
	case 0:
		{
			fmt.Println("未设置group,将使用xread监听")
		}
	case 1:
		{
			s.ConsumerGroup = group[0]
		}
	default:
		{
			fmt.Println("只支持至多1个group,当作未设置group处理")
		}
	}
	return s
}

func (consumer *streamConsumer) readOne(ctx context.Context, conn *redis.Client) ([]redis.XStream, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	fmt.Printf("PoolStats: %+v \n", conn.PoolStats())
	if consumer.ConsumerGroup == "" {
		streams := []string{}
		for _, topic := range consumer.Topics {
			streams = append(streams, topic)
		}
		for range consumer.Topics {
			streams = append(streams, consumer.Start)
		}
		args := redis.XReadArgs{
			Streams: streams,
			Count:   consumer.Count,
			Block:   consumer.Block,
		}
		return conn.WithContext(ctx).XRead(&args).Result()
	}
	streams := []string{}
	for _, topic := range consumer.Topics {
		streams = append(streams, topic)
	}
	for range consumer.Topics {
		streams = append(streams, consumer.Start)
	}
	args := redis.XReadGroupArgs{
		Group:    consumer.ConsumerGroup,
		Consumer: consumer.ConsumerName,
		Streams:  consumer.Topics,
		Count:    consumer.Count,
		Block:    consumer.Block,
		NoAck:    consumer.NoAck,
	}
	return conn.XReadGroup(&args).Result()
}

//Read 订阅流,count可以
func (consumer *streamConsumer) Read(ctx context.Context) ([]redis.XStream, error) {
	if !consumer.proxy.IsOk() {
		return nil, ErrHelperNotInited
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		return nil, err
	}
	return consumer.readOne(ctx, conn)
}

//Subscribe 订阅流,count可以
func (consumer *streamConsumer) Ack(ctx context.Context, ids ...string) error {
	if !consumer.proxy.IsOk() {
		return ErrHelperNotInited
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		return err
	}
	pipe := conn.WithContext(ctx).TxPipeline()
	for _, topic := range consumer.Topics {
		pipe.XAck(topic, consumer.ConsumerGroup, ids...)
	}
	_, err = pipe.Exec()
	if err != nil {
		return err
	}
	return nil
}

//Read 订阅流,count可以
func (consumer *streamConsumer) Subscribe(ctx context.Context) (<-chan redis.XStream, error) {

	if !consumer.proxy.IsOk() {
		fmt.Println("consumer.proxy.IsOk error: ", ErrHelperNotInited.Error())
		return nil, ErrHelperNotInited
	}
	if !(consumer.Block >= 0) {
		fmt.Println("consumer must blocked", ErrStreamConsumerNotBlocked.Error())
		return nil, ErrStreamConsumerNotBlocked
	}
	if consumer.isSubscribed {
		return nil, ErrIsAlreadySubscribed
	}
	conn, err := consumer.proxy.GetConn()
	if err != nil {
		fmt.Println("GetConn error: ", err.Error())
		return nil, err
	}
	ch := make(chan redis.XStream)
	go func() {
	Loop:
		for {
			select {
			case rec := <-consumer.stopch:
				{
					fmt.Println("exiting...")
					if rec == Done {
						close(ch)
						consumer.isSubscribed = false
						break Loop
					}
				}
			default:
				{
					res, err := consumer.readOne(ctx, conn)
					if err != nil {
						if err == redis.Nil {
							continue
						} else {
							fmt.Println("read one error: ", err.Error())
							close(ch)
							//panic(err)
							consumer.isSubscribed = false
							break Loop
						}
					} else {
						for _, message := range res {
							ch <- message
						}
					}
				}
			}
		}
	}()
	consumer.isSubscribed = true
	return ch, nil
}
func (consumer *streamConsumer) Close() error {
	if !consumer.isSubscribed {
		return ErrIsAlreadyUnSubscribed
	}
	consumer.stopch <- Done
	return nil
}
