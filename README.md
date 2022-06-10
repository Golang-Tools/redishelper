# redishelper

`github.com/go-redis/redis/v8`的帮助程序

主要包括组件:

+ `redisproxy`,redis客户端的代理对象,用于代理`github.com/go-redis/redis/v8`的`UniversalClient`
+ `exthelper`,redis客户端的扩展管理对象
+ `cellhelper`,redis扩展[redis-cell](https://github.com/brandur/redis-cell)的帮助模块
+ `scaner`,scan遍历全局keys的遍历器对象
+ `pubsubhelper`,pubsub模式的客户端,满足`pchelper`定义的生产者接口`ProducerInterface`和消费者接口`ConsumerInterface`
+ `queuehelper`,redis双端队列的客户端,满足`pchelper`定义的生产者接口`ProducerInterface`和消费者接口`ConsumerInterface`
+ `streamhelper`,redis的stream数据结构的客户端,满足`pchelper`定义的生产者接口`ProducerInterface`和消费者接口`ConsumerInterface`,同时提供stream结构的管理对象
+ `incrlimiter`,使用redis的string数据结构的incr原子自增特性构造的限流器,满足`limiterhelper`定义的限流器接口`LimiterInterface`
+ `celllimiter`,使用redis的string数据结构的incr原子自增特性构造的限流器,满足`limiterhelper`定义的限流器接口`LimiterInterface`
+ `lock`,使用redis构造的分布式锁结构
+ `cache`,利用redis构造的分布式缓存,可以搭配`lock`模块中定义的`LockInterface`接口的实现和`limiterhelper`定义的限流器接口`LimiterInterface`的实现增强功能
+ `keycounter`,利用redis的string数据结构的incr原子自增特性构造的分布式计数器,满足模块`counterhelper`定义的接口`CounterInterface`
+ `hashcounter`,利用redis的hashmap数据结构的incr原子自增特性构造的分布式计数器,满足模块`counterhelper`定义的接口`CounterInterface`
+ `ranker`,利用redis的有序结合数据结构构造的分布式排序器
+ `hypercount`,利用redis的`hypercount`构造的分布式大规模计数器,适用于统计日活等操作.
+ `bitmapset`,利用redis的`bitmap`构造的分布式集合,适合用于去重操作,签到操作等
