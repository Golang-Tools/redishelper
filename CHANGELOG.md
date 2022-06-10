# v2.0.1

## 接口重构说明

改变了本项目的设计思路,现在本项目主要是提供借助redis实现的组件以及一些便捷工具.因此废除了v1版本中的核心组件`clientkey`和`clientkeybatch`.

所有的子模块初始化都不再显示的依赖核心组件的实例,而是使用redis的客户端就可以直接创建.

在参数方面全部改用grpc风格的可选参数设计.

## 新增功能

+ `keyspace_notifications`,用于管理`keyspace_notifications`的设置,监听`keyspace_notifications`事件

# v2.0.0

使用go 1.18+语法重构项目

# v0.0.8

修改注释以避免被swag误识别

# v0.0.7

## 接口优化

`proxy`的参数`WithQueryTimeout`改为`WithQueryTimeoutMS`明确单位为ms

# v0.0.6

## 依赖更新

+ `github.com/Golang-Tools/loggerhelper@v0.0.4`
+ `github.com/go-redis/redis/v8@v8.11.3`
+ `github.com/json-iterator/go@v1.1.12`
+ `github.com/vmihailenco/msgpack/v5@v5.3.4`

# v0.0.5

## 增加特性

+ `proxy`将增加接口`NewCtx`用于创建请求上下文

# v0.0.4

## 修正实现

+ `proxy`将统一init接口,改用opt形式作为额外参数

# v0.0.3

## 性能优化

+ `lock`的主动删除操作改为原子操作

## 修正实现

+ `broker/stream`中`Stream.CreateGroup`接口新增参数`autocreate bool`用于指定是否在key不存在时新增key

## 新增实现

+ 实现了使用scan遍历查询的接口

# v0.0.2

## 修正实现

+ cache现在修正了缓存的流程,更新操作被拆分为了主动更新(`ActiveUpdate()`)和懒更新(`lazyUpdate(ctx context.Context, force ForceLevelType)`)两种模式,`Get`方法会调用懒更新接口,自动更新则会调用主动更新接口,主动更新接口也可以被用于监听消息作为回调函数

## 新增实现

+ 新增专用接口`github.com/Golang-Tools/redishelper/keyspace_notifications`用于监听redis的键的行为通知
+ 新增新增`github.com/Golang-Tools/redishelper/ext`模块用于实现检查已加载模块的功能
+ 新增`github.com/Golang-Tools/redishelper/ext/redis-cell`模块用于对[redis-cell](https://github.com/brandur/redis-cell)做支持,同时用它实现了一个限流器

# v0.0.1

初始化了项目,新增了如下结构:

+ `github.com/Golang-Tools/redishelper/bitmap`封装了分布式位图,可以用于相对低空间占用的去重并且满足全部集合操作满足`CanBeSet`和`CanBeClientKey`接口
+ `github.com/Golang-Tools/redishelper/broker`封装了redis支持的3种可以用作中间人的数据结构(queue,stream,pubsub),统一按生产者(`CanBeProducer`和`CanBeClientKey`接口)消费者(`CanBeConsumer`和`CanBeClientKeyBatch`接口)模式进行了封装
+ `github.com/Golang-Tools/redishelper/cache`封装了分布式缓存,支持使用分布式锁和分布式限制器控制缓存的执行策略,同时支持自动更新同步缓存内容,满足`CanBeClientKey`接口
+ `github.com/Golang-Tools/redishelper/counter`对象封装了分布式累加器,它也可以用于作为分布式系统的id生成器,满足接口`CanBeCounter`和`CanBeClientKey`
+ `github.com/Golang-Tools/redishelper/hypercount`用于为大规模数据做去重计数和一部分集合操作,满足`CanBeDisctinctCounter`和`CanBeClientKey`接口
+ `github.com/Golang-Tools/redishelper/limiter`限制器,用于限流和缓存防击穿等,满足接口`CanBeLimiter`
+ `github.com/Golang-Tools/redishelper/lock`分布式锁,用于避免分布式系统中的资源争抢,满足接口`Canlock`和`CanBeClientKey`
+ `github.com/Golang-Tools/redishelper/namespace`用于构造命名空间的辅助工具
+ `github.com/Golang-Tools/redishelper/proxy`用于给`github.com/go-redis/redis/v8`中的客户端对象提供代理,满足接口`redis.UniversalClient`
+ `github.com/Golang-Tools/redishelper/randomkey`用于生成随机的key
+ `github.com/Golang-Tools/redishelper/ranker`排序工具封装,满足接口`CanBeClientKey`
