---
title: FAQ
layout: plain_cn
---
## 性能问题
参考性能优化

## 运行时topology的task列表中报"task is dead"错误
有几个原因可能导致出现这个错误：

1. task心跳超时，导致nimbus主动kill这个task所在的worker
2. task对应的 bolt/spout 中的open/prepare/execute/nextTuple等，没有对异常做try...catch，导致抛出异常，导致task挂掉。**这里要注意一下，一个worker中任意一个task如果没有做异常处理，会导致整个worker挂掉，会导致该worker中其他task也报Task is dead**，所以在jstorm的应用代码中，**强烈建议在所有的方法中都加上try...catch**。

具体排查可以这么来做：

1. 如果task是每隔4分钟左右有规律地挂掉，那么基本可以确定是task心跳超时导致的，可以直接跳到3
2. 查看worker日志，在挂掉的时间点是否有异常。但是注意要看挂掉的那个worker的日志，而不是重新起来之后新的worker的日志，因为worker重新起来之后可能位于不同的机器上。
3. 如果worker日志没有异常，那么可以看一下集群nimbus的日志，搜一下："Update taskheartbeat"，然后找到挂掉的worker所对应的topology Id，看看最后更新心跳的时间是什么时候。对比一下task心跳超时的配置（nimbus.task.timeout.secs），如果worker挂掉的时间 - 最后一次更新心跳的时间 > task心跳超时，那么基本上可以确定是因为task心跳超时被kill了。这有几种可能：

* 执行队列被阻塞了，一直没有返回； 
* worker发生了FGC，这会导致正常的线程都被停住，从而导致心跳超时。这时要查看一下对应的GC日志，看那个时间点附近有没有FGC； 
* worker/task抛出了未处理的异常，如OutOfMemoryError之类的
* 最后也有可能是worker一直没起来, worker心跳超时

## [Netty-Client-boss-1] Failed to reconnect ...[15], /10.1.11.1:6801, channel, cause: java.net.ConnectException: Connection refused错误
这个日志一般只会在task/worker挂掉的时候才会出现，因为挂掉的worker对应的端口已经被释放，所以会出现连接拒绝。具体排查见上面的"task is dead“

## task报“queue is full”
JStorm bolt/spout 中有三个基本的队列： Deserialize Queue ---> Executor Queue ---> Serialize Queue。每一个队列都有满的可能。
如果是 serializeQueue is full，那么可能是序列化对象太大，序列化耗时太长。可以精简传输对象。
如果是deserialize queue is full， 或是execute queue is full。 2个原因都是一样的。都是下游bolt处理速度跟不上上游spout或bolt的发送速度。
解决办法：

1. 判断是不是一个常态问题以及是不是大面积发生，如果就1个或2个task出现，并且没有引起worker out of memory，其实是可以忽略的。
2. 如果一个component大面积发生task 队列满， 或因为task 满导致worker out of memory， 就需要解决处理速度更不上的问题。

怎么解决，请参考 `性能调优` 尤其是下游的bolt的处理能力提上来， 最简单的办法是增加并发， 如果增加并发不能解决问题， 请参考`性能调优`寻找优化点。

## 提交topology后task状态一直是starting
首先请到topology页面点task的worker log，看有没有日志

   - 如果有worker log，请看看里面是否有异常。确认一下你的所有方法中，如open/prepare/execute/nextTuple中，有没有做try...catch，如果你抛出了异常并且没有做处理，jstorm默认就会认为这个worker有问题，这样会导致整个worker都挂掉了。
   - 如果没有，则可能有以下几个原因：
	- 你的topology请求的memory过多，导致分配不出需要的内存（包括：worker.memory.size配置，JVM参数中-Xmx -Xms等的配置）。
	- supervisor机器的磁盘满了，或者其他机器原因。
   - 还有一些常见的错误，如jvm参数设置不正确（比如-Xmn > -Xmx，使用了对应jdk不支持的JVM参数等）；jar包冲突（如日志冲突）等

## 资源不够
当报告 ”No supervisor resource is enough for component “， 则意味着资源不够
如果是仅仅是测试环境，可以将supervisor的cpu 和memory slot设置大， 

在jstorm中， 一个task默认会消耗一个cpu slot和一个memory slot， 而一台机器上默认的cpu slot是(cpu 核数 -1）， memory slot数（物理内存大小 * 75%/1g）, 如果一个worker上运行task比较多时，需要将memory slot size设小（默认是1G）， 比如512M, memory.slot.per.size: 535298048

```
 #if it is null, then it will be detect by system
 supervisor.cpu.slot.num: null

 #if it is null, then it will be detect by system
 supervisor.mem.slot.num: null

 #support disk slot
 #if it is null, it will use $(storm.local.dir)/worker_shared_data
 supervisor.disk.slot: null
```

## 提交topology时报：org.apache.thrift.transport.TTransportException: Frame size (17302738) larger than max length (16384000)!
这个问题的原因是序列化后的topology对象过大导致的，通常可能是你在spout/bolt中创建了一个大对象（比如bitmap, 大数组等），导致序列化后对象的大小超过了thrift的max frame size（thrift中16384000这个值是写死的，只能调小不能调大）。在JStorm中，如果需要在spout/bolt中创建大对象，建议是在open/prepare方法中来做，延迟对象的创建时间。参见：https://github.com/alibaba/jstorm/issues/230

## 序列化问题
所有spout，bolt，configuration， 发送的消息（Tuple）都必须实现Serializable， 否则就会出现序列化错误.

如果是spout或bolt的成员变量没有实现Serializable时，但又必须使用时， 
可以对该变量申明时，增加transient 修饰符， 然后在open或prepare时，进行实例化

![seriliazble_error]({{site.baseurl}}/img/FAQ/serializable_error.jpg)

## 日志冲突
JStorm 0.9.x系列使用log4j作为日志系统，2.x系列使用logback作为日志系统。
但是不管使用哪个版本的jstorm，都需要注意的是不能使用冲突的日志依赖。比如log4j-over-slf4j和slf4j-log4j12是冲突的，它们是肯定不能共存的，否则会出现类似这个错误：

```
SLF4J: Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError. 
SLF4J: See also 
http://www.slf4j.org/codes.html#log4jDelegationLoop for more details.
Exception in thread "main" java.lang.ExceptionInInitializerError
        at org.apache.log4j.Logger.getLogger(Logger.java:39)
        at org.apache.log4j.Logger.getLogger(Logger.java:43)
        at com.alibaba.jstorm.daemon.worker.Worker.<clinit>(Worker.java:32)
Caused by: java.lang.IllegalStateException: Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError. See also 
http://www.slf4j.org/codes.html#log4jDelegationLoop for more details.
        at org.apache.log4j.Log4jLoggerFactory.<clinit>(Log4jLoggerFactory.java:49)
        ... 3 more
Could not find the main class: com.alibaba.jstorm.daemon.worker.Worker.  Program will exit.
```

具体地来说，jstorm 0.9.x依赖了log4j, slf4j-log4j12，因此如果使用了0.9.x版本，你的应用代码中必须要排除掉log4j-over-slf4j的依赖。
同样地，jstorm 2.x依赖了logback, log4j-over-slf4j，如果使用了这个版本，你的应用代码中需要排除掉slf4j-log4j12的依赖。


## 类冲突
如果应用程序使用和JStorm相同的jar 但版本不一样时，建议打开classloader，
修改配置文件

```
topology.enable.classloader: true
```

或者

```
ConfigExtension.setEnableTopologyClassLoader(conf, true);
```

JStorm默认是关掉classloader，因此JStorm会强制使用JStorm依赖的jar

## 提交任务后，等待几分钟后，web ui始终没有显示对应的task
有3种情况：
* 用户程序初始化太慢
如果有用户程序的日志输出，则表明是用户的初始化太慢或者出错，查看日志即可。 另外对于MetaQ 1.x的应用程序，Spout会recover ~/.meta_recover/目录下文件，可以直接删除这些消费失败的问题，加速启动。

* 通常是用户jar冲突或初始化发生问题
打开supervisor 日志，找出启动worker命令，单独执行，然后检查是否有问题。类似下图：

![fail_start_worker]({{site.baseurl}}/img/FAQ/fail_start_worker.jpg)

* 检查是不是storm和jstorm使用相同的本地目录
检查配置项 ”storm.local.dir“， 是不是storm和jstorm使用相同的本地目录，如果相同，则将二者分开

## 提示端口被绑定
有2种情况：
* 多个worker抢占一个端口
假设是6800 端口被占， 可以执行命令 “ps -ef|grep 6800” 检查是否有多个进程， 如果有多个进程，则手动杀死他们

* 系统打开太多的connection
Linux对外连接端口数限制，TCP client对外发起连接数达到28000左右时，就开始大量抛异常，需要

```
 # echo "10000 65535" > /proc/sys/net/ipv4/ip_local_port_range
```

其他问题，可以入QQ群进行咨询228374502
