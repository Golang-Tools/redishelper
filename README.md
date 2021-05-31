# redishelper

`github.com/go-redis/redis/v8`的帮助程序

## 特性

+ 提供一个未设置被代理对象的默认代理对象`Proxy`
+ 可以代理`Client`,`ClusterClient`,`FailoverClusterClient`
+ 设置被代理对象时可以串行或者并行的执行注册的回调函数,回调函数只能在未设置被代理客户端时注册
+ 代理对象一旦被设置则无法替换
+ 封装了一些常用的工具
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
    + `github.com/Golang-Tools/redishelper/scanfinder`封装了使用scan遍历全局keys的方法