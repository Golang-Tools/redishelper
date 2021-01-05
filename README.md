# redishelper

`github.com/go-redis/redis/v8`的帮助程序

## 特性

+ 提供一个未设置被代理对象的默认代理对象`Proxy`
+ 可以代理`Client`,`ClusterClient`,`FailoverClusterClient`
+ 设置被代理对象时可以串行或者并行的执行注册的回调函数,回调函数只能在未设置被代理客户端时注册
+ 代理对象一旦被设置则无法替换
+ 封装了一些常用的分布式工具
    + `github.com/Golang-Tools/redishelper/randomkey`用于生成随机的key
    + `github.com/Golang-Tools/redishelper/namespace`用于构造命名空间
    + `github.com/Golang-Tools/redishelper/proxy`用于给`github.com/go-redis/redis/v8`中的客户端对象提供代理
    + `bitmap`封装了分布式位图,现在可以更加自然的操作bitmap作为去重工具了
    + `counter`对象封装了分布式累加器
    + `ranker`全局排序工具
    + `queue`封装了list结构,可以用于做消息队列
