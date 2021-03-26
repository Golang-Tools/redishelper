package stream

//流对象
import (
	"context"
	"time"

	"github.com/Golang-Tools/redishelper/clientkey"
	set "github.com/deckarep/golang-set"
	"github.com/go-redis/redis/v8"
)

// //Handdler 获取到stream结果的回调
// type Handdler func(*redis.XStream) error

// //TopicInfo 消费者监听时指定的topic信息
// type TopicInfo struct {
// 	Topic string
// 	Start string
// }

//Stream 流对象
type Stream struct {
	MaxLen int64
	Strict bool
	*clientkey.ClientKey
}

//New 创建一个新的queue对象
//@params k *clientkey.ClientKey redis客户端的键对象
func New(k *clientkey.ClientKey, maxlen int64, strict bool) *Stream {
	c := new(Stream)
	c.ClientKey = k
	c.MaxLen = maxlen
	c.Strict = strict
	return c
}

//管理流

//Len 查看流的当前长度
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (s *Stream) Len(ctx context.Context) (int64, error) {
	return s.Client.XLen(ctx, s.Key).Result()
}

//Trim 为流扩容
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 扩容到多长
//@params strict bool 是否严格控制长度
func (s *Stream) Trim(ctx context.Context, count int64, strict bool) (int64, error) {
	if strict {
		return s.Client.XTrim(ctx, s.Key, count).Result()
	}
	return s.Client.XTrimApprox(ctx, s.Key, count).Result()
}

//Delete 设置标志位标识删除流中指定id的数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params ids ...string 要删除的id列表
func (s *Stream) Delete(ctx context.Context, ids ...string) error {
	_, err := s.Client.XDel(ctx, s.Key, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Range 获取消息列表,会自动过滤已经删除的消息
//@params ctx context.Context 请求的上下文
//@params start string 开始位置,`-`表示最小值, `+`表示最大值,可以指定毫秒级时间戳,也可以指定特定消息id
//@params start string 开始位置,`-`表示最小值, `+`表示最大值,可以指定毫秒级时间戳,也可以指定特定消息id
//@params stop string 结束位置,`-`表示最小值, `+`表示最大值,可以指定毫秒级时间戳,也可以指定特定消息id
func (s *Stream) Range(ctx context.Context, start, stop string) ([]redis.XMessage, error) {
	return s.Client.XRange(ctx, s.Key, start, stop).Result()
}

// 管理流上注册的消费者组

//GroupInfos 获取主题流中注册的消费者组信息
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (s *Stream) GroupInfos(ctx context.Context) ([]redis.XInfoGroup, error) {
	return s.Client.XInfoGroups(ctx, s.Key).Result()
}

//HasGroup 判断消费者组是否在Stream上
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名
func (s *Stream) HasGroup(ctx context.Context, groupname string) (bool, error) {
	groups, err := s.GroupInfos(ctx)
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

//HasGroups 判断消费者组是否都在在Stream上
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupnames ...string 消费者组名列表
func (s *Stream) HasGroups(ctx context.Context, groupnames ...string) (bool, error) {
	if len(groupnames) <= 0 {
		return false, ErrStreamNeedToPointOutGroups
	}
	groups, err := s.GroupInfos(ctx)
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

//CreateGroup 为指定消费者在指定的Stream上创建消费者组
//如果stream不存在则创建之
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params start string 开始位置,"$"表示最近,"0"表示最初.也可以填入正常的id
func (s *Stream) CreateGroup(ctx context.Context, groupname, start string) (string, error) {
	return s.Client.XGroupCreateMkStream(ctx, s.Key, groupname, start).Result()
}

//DeleteGroup 为指定消费者在指定的Stream上删除消费者组
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
func (s *Stream) DeleteGroup(ctx context.Context, groupname string) (int64, error) {
	return s.Client.XGroupDestroy(ctx, s.Key, groupname).Result()
}

//DeleteConsumerFromGroup 在指定的Stream上删除指定消费者组中的指定消费者
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params groupname string 消费者组名列表
func (s *Stream) DeleteConsumerFromGroup(ctx context.Context, groupname, consumername string) (int64, error) {
	return s.Client.XGroupDelConsumer(ctx, s.Key, groupname, consumername).Result()
}

//SetGroupStartAt 设置指定消费者组在主题流中的读取起始位置
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params start string 开始位置
func (s *Stream) SetGroupStartAt(ctx context.Context, groupname, start string) (string, error) {
	return s.Client.XGroupSetID(ctx, s.Key, groupname, start).Result()
}

//Pending 查看消费组中等待确认的消息列表
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
func (s *Stream) Pending(ctx context.Context, groupname string) (*redis.XPending, error) {
	return s.Client.XPending(ctx, s.Key, groupname).Result()
}

//Move 转移消息的所有权给用户组中的某个用户
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params toconsumer string 要转移给所有权的消费者
//@params minIdle time.Duration 被转移出去的消息等待时间最小值,为0则时间计数被重置
//@params ids ...string 要转移所有权的消息id
func (s *Stream) Move(ctx context.Context, groupname, toconsumer string, minIdle time.Duration, ids ...string) ([]redis.XMessage, error) {
	args := redis.XClaimArgs{
		Stream:   s.Key,
		Group:    groupname,
		Consumer: toconsumer,
		MinIdle:  minIdle,
		Messages: ids,
	}
	return s.Client.XClaim(ctx, &args).Result()
}

//MoveJustID 转移消息的所有权给用户组中的某个用户
//这个命令和Move区别在于返回值只有消息id
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params toconsumer string 要转移给所有权的消费者
//@params minIdle time.Duration 被转移出去的消息等待时间最小值,为0则时间计数被重置
//@params ids ...string 要转移所有权的消息id
func (s *Stream) MoveJustID(ctx context.Context, groupname, toconsumer string, minIdle time.Duration, ids ...string) ([]string, error) {
	args := redis.XClaimArgs{
		Stream:   s.Key,
		Group:    groupname,
		Consumer: toconsumer,
		MinIdle:  minIdle,
		Messages: ids,
	}
	return s.Client.XClaimJustID(ctx, &args).Result()
}

//Ack 手工确认组已经消耗了消息
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params ids ...string 确认被消耗的id列表
func (s *Stream) Ack(ctx context.Context, groupname string, ids ...string) error {
	_, err := s.Client.XAck(ctx, s.Key, groupname, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}
