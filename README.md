# redishelper

`github.com/go-redis/redis/v8`的帮助程序

## 特性

+ 提供一个未设置被代理对象的默认代理对象`Proxy`
+ 可以代理`Client`,`ClusterClient`,`FailoverClusterClient`
+ 设置被代理对象时可以串行或者并行的执行注册的回调函数,回调函数只能在未设置被代理客户端时注册
+ 代理对象一旦被设置则无法替换
+ 封装了一些常用的工具
    + `github.com/Golang-Tools/redishelper/randomkey`用于生成随机的key
    + `github.com/Golang-Tools/redishelper/namespace`用于构造命名空间
    + `github.com/Golang-Tools/redishelper/proxy`用于给`github.com/go-redis/redis/v8`中的客户端对象提供代理
    + `github.com/Golang-Tools/redishelper/bitmap`封装了分布式位图,现在可以更加自然的操作bitmap作为去重工具了
    + `github.com/Golang-Tools/redishelper/counter`对象封装了分布式累加器
    + `github.com/Golang-Tools/redishelper/ranker`全局排序工具
    + `github.com/Golang-Tools/redishelper/queue`封装了list结构,可以用于做消息队列
    + `github.com/Golang-Tools/redishelper/pubsub`广播器的封装
    + `github.com/Golang-Tools/redishelper/stream`封装了Stream结构,可以用于代替kafka
