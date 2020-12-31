# redishelper

`github.com/go-redis/redis/v8`的代理对象

## 特性

+ 提供一个未设置被代理对象的默认代理对象`Proxy`
+ 可以代理`Client`,`ClusterClient`,`FailoverClusterClient`
+ 设置被代理对象时可以串行或者并行的执行注册的回调函数,回调函数只能在未设置被代理客户端时注册
+ 代理对象一旦被设置则无法替换
+ 以`Bitmap`开头的方法封装了位操作,现在可以更加自然的操作bitmap作为去重工具了



