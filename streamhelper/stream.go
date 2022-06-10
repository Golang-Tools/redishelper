package streamhelper

//流对象
import (
	"context"
	"time"

	"github.com/Golang-Tools/optparams"
	set "github.com/deckarep/golang-set/v2"
	"github.com/go-redis/redis/v8"
)

//Stream 流对象
type Stream struct {
	Name string
	cli  redis.UniversalClient
}

//NewStream 创建一个新的流对象
//@params cli redis.UniversalClient redis客户端的对象
//@params name string 流名
func NewStream(cli redis.UniversalClient, name string) *Stream {
	c := new(Stream)
	c.Name = name
	c.cli = cli
	return c
}

//管理流

//Len 查看流的当前长度
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (s *Stream) Len(ctx context.Context) (int64, error) {
	return s.cli.XLen(ctx, s.Name).Result()
}

type trimOpt struct {
	Strict bool
}

//WithStrict Trim方法的参数,用于设置严格控制长度
func WithStrict() optparams.Option[trimOpt] {
	return optparams.NewFuncOption(func(o *trimOpt) {
		o.Strict = true
	})
}

//Trim 为流扩容
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params count int64 扩容到多长
//@params strict bool 是否严格控制长度
func (s *Stream) Trim(ctx context.Context, count int64, opts ...optparams.Option[trimOpt]) (int64, error) {
	defOpt := trimOpt{}
	optparams.GetOption(&defOpt, opts...)
	if defOpt.Strict {
		return s.cli.XTrim(ctx, s.Name, count).Result()
	}
	return s.cli.XTrimApprox(ctx, s.Name, count).Result()
}

//Delete 设置标志位标识删除流中指定id的数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params ids ...string 要删除的消息id列表
func (s *Stream) Delete(ctx context.Context, ids ...string) error {
	_, err := s.cli.XDel(ctx, s.Name, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}

//Range 获取消息列表,会自动过滤已经删除的消息
//@params ctx context.Context 请求的上下文
//@params start string 开始位置,`-`表示最小值, `+`表示最大值,可以指定毫秒级时间戳,也可以指定特定消息id
//@params stop string 结束位置,`-`表示最小值, `+`表示最大值,可以指定毫秒级时间戳,也可以指定特定消息id
func (s *Stream) Range(ctx context.Context, start, stop string) ([]redis.XMessage, error) {
	return s.cli.XRange(ctx, s.Name, start, stop).Result()
}

// 管理流上注册的消费者组

//GroupInfos 获取主题流中注册的消费者组信息
//@params ctx context.Context 上下文信息,用于控制请求的结束
func (s *Stream) GroupInfos(ctx context.Context) ([]redis.XInfoGroup, error) {
	return s.cli.XInfoGroups(ctx, s.Name).Result()
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
	groupnamesSet := set.NewSet(groupnames...)
	hasgroupSet := set.NewSet[string]()
	for _, group := range groups {
		hasgroupSet.Add(group.Name)
	}
	hasgroupSet.IsSuperset(groupnamesSet)
	return hasgroupSet.IsSuperset(groupnamesSet), nil
}

type createGroupOpt struct {
	Start      string
	Autocreate bool
}

//WithAutocreate CreateGroup方法的参数,用于设置是否自动创建流
func WithAutocreate() optparams.Option[createGroupOpt] {
	return optparams.NewFuncOption(func(o *createGroupOpt) {
		o.Autocreate = true
	})
}

//WithStartEarliest CreateGroup方法的参数,用于设置创建流后流的默认读取位置为最早数据
func WithStartEarliest() optparams.Option[createGroupOpt] {
	return optparams.NewFuncOption(func(o *createGroupOpt) {
		o.Start = "0"
	})
}

//WithStartLatest CreateGroup方法的参数,用于设置创建流后流的默认读取位置为最新数据
func WithStartLatest() optparams.Option[createGroupOpt] {
	return optparams.NewFuncOption(func(o *createGroupOpt) {
		o.Start = "$"
	})
}

//WithStartLatest CreateGroup方法的参数,用于设置创建流后流的默认读取位置为指定id
func WithStartAtID(id string) optparams.Option[createGroupOpt] {
	return optparams.NewFuncOption(func(o *createGroupOpt) {
		o.Start = id
	})
}

//CreateGroup 为指定消费者在指定的Stream上创建消费者组,创建流后流的默认读取位置为最新数据
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
func (s *Stream) CreateGroup(ctx context.Context, groupname string, opts ...optparams.Option[createGroupOpt]) (string, error) {
	defOpt := createGroupOpt{
		Start: "$",
	}
	optparams.GetOption(&defOpt, opts...)
	if defOpt.Autocreate {
		autocreatescript := redis.NewScript(`
			if redis.call("EXISTS", KEYS[1]) == 1 then
				return redis.call("XGROUP", "CREATE", KEYS[1], ARGV[1], ARGV[2])
			else
				return redis.call("XGROUP", "CREATE", KEYS[1], ARGV[1], ARGV[2], "MKSTREAM")
			end
		`)
		res, err := autocreatescript.Run(ctx, s.cli, []string{s.Name}, groupname, defOpt.Start).Result()
		if err != nil {
			return "", err
		}
		return res.(string), nil
	} else {
		return s.cli.XGroupCreate(ctx, s.Name, groupname, defOpt.Start).Result()
	}
}

//DeleteGroup 为指定消费者在指定的Stream上删除消费者组
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
func (s *Stream) DeleteGroup(ctx context.Context, groupname string) (int64, error) {
	return s.cli.XGroupDestroy(ctx, s.Name, groupname).Result()
}

//DeleteConsumerFromGroup 在指定的Stream上删除指定消费者组中的指定消费者
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params groupname string 消费者组名列表
func (s *Stream) DeleteConsumerFromGroup(ctx context.Context, groupname, consumername string) (int64, error) {
	return s.cli.XGroupDelConsumer(ctx, s.Name, groupname, consumername).Result()
}

//SetGroupStartAt 设置指定消费者组在主题流中的读取起始位置
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params start string 开始位置
func (s *Stream) SetGroupStartAt(ctx context.Context, groupname, start string) (string, error) {
	return s.cli.XGroupSetID(ctx, s.Name, groupname, start).Result()
}

//Pending 查看消费组中等待确认的消息列表
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
func (s *Stream) Pending(ctx context.Context, groupname string) (*redis.XPending, error) {
	return s.cli.XPending(ctx, s.Name, groupname).Result()
}

//Move 转移消息的所有权给用户组中的某个用户
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params toconsumer string 要转移给所有权的消费者
//@params minIdle time.Duration 被转移出去的消息等待时间最小值,为0则时间计数被重置
//@params ids ...string 要转移所有权的消息id
func (s *Stream) Move(ctx context.Context, groupname, toconsumer string, minIdle time.Duration, ids ...string) ([]redis.XMessage, error) {
	args := redis.XClaimArgs{
		Stream:   s.Name,
		Group:    groupname,
		Consumer: toconsumer,
		MinIdle:  minIdle,
		Messages: ids,
	}
	return s.cli.XClaim(ctx, &args).Result()
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
		Stream:   s.Name,
		Group:    groupname,
		Consumer: toconsumer,
		MinIdle:  minIdle,
		Messages: ids,
	}
	return s.cli.XClaimJustID(ctx, &args).Result()
}

//Ack 手工确认组已经消耗了消息
//@params ctx context.Context 上下文信息,用于控制请求的结束
//@params groupname string 消费者组名列表
//@params ids ...string 确认被消耗的id列表
func (s *Stream) Ack(ctx context.Context, groupname string, ids ...string) error {
	_, err := s.cli.XAck(ctx, s.Name, groupname, ids...).Result()
	if err != nil {
		return err
	}
	return nil
}
