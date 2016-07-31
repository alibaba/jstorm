---
title:  "Basic Conception"
# Top-level navigation
top-nav-group: QuickStart_cn
top-nav-pos: 1
top-nav-title: 5分钟基础概念(新手第一篇)
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}

本文主要讲一下JStorm的基本概念，让你在5分钟内对JStorm有一个大体的了解

# JStorm 是一个分布式实时计算引擎。 

JStorm 是一个类似Hadoop MapReduce的系统， 用户按照指定的接口实现一个任务，然后将这个任务递交给JStorm系统，JStorm将这个任务跑起来，并且按7 * 24小时运行起来，一旦中间一个Worker 发生意外故障， 调度器立即分配一个新的Worker替换这个失效的Worker。

因此，从应用的角度，JStorm应用是一种遵守某种编程规范的分布式应用。从系统角度， JStorm是一套类似MapReduce的调度系统。 从数据的角度，JStorm是一套基于流水线的消息处理机制。

实时计算现在是大数据领域中最火爆的一个方向，因为人们对数据的要求越来越高，实时性要求也越来越快，传统的Hadoop MapReduce，逐渐满足不了需求，因此在这个领域需求不断。

**Storm组件和Hadoop组件对比**

| | JStorm | Hadoop |
| ------------- | ------------- | ------------- |
|角色|Nimbus|JobTracker|
||Supervisor|TaskTracker|
||Worker|Child|
|应用名称|Topology|Job|
|编程接口|Spout/Bolt|Mapper/Reducer|

# 优点
在Storm和JStorm出现以前，市面上出现很多实时计算引擎，但自Storm和JStorm出现后，基本上可以说一统江湖：
究其优点:

* 开发非常迅速：接口简单，容易上手，只要遵守Topology、Spout和Bolt的编程规范即可开发出一个扩展性极好的应用，底层RPC、Worker之间冗余，数据分流之类的动作完全不用考虑
* 扩展性极好：当一级处理单元速度，直接配置一下并发数，即可线性扩展性能
* 健壮强：当Worker失效或机器出现故障时， 自动分配新的Worker替换失效Worker
* 数据准确性：可以采用Ack机制，保证数据不丢失。 如果对精度有更多一步要求，采用事务机制，保证数据准确。
* 实时性高： JStorm 的设计偏向单行记录，因此，在时延较同类产品更低

# 应用场景

JStorm处理数据的方式是基于消息的流水线处理， 因此特别**适合无状态计算**，也就是计算单元的依赖的数据全部在接受的消息中可以找到， 并且最好一个数据流不依赖另外一个数据流。

因此，常常用于:

* 日志分析，从日志中分析出特定的数据，并将分析的结果存入外部存储器如数据库。目前，主流日志分析技术就使用JStorm或Storm
* 管道系统， 将一个数据从一个系统传输到另外一个系统， 比如将数据库同步到Hadoop
* 消息转化器， 将接受到的消息按照某种格式进行转化，存储到另外一个系统如消息中间件
* 统计分析器， 从日志或消息中，提炼出某个字段，然后做count或sum计算，最后将统计值存入外部存储器。中间处理过程可能更复杂。
* 实时推荐系统， 将推荐算法运行在jstorm中，达到秒级的推荐效果


# 基本概念

首先，JStorm有点类似于Hadoop的MR（Map-Reduce），但是区别在于，hadoop的MR，提交到hadoop的MR job，执行完就结束了，进程就退出了，而一个JStorm任务（JStorm中称为topology），是7*24小时永远在运行的，除非用户主动kill。

## JStorm组件

接下来是一张比较经典的Storm的大致的结构图（跟JStorm一样）：

<p align="center">
<img src="http://storm.apache.org/images/storm-flow.png" width="600px" text-align:center>
</p>

图中的水龙头（好吧，有点俗）就被称作spout，闪电被称作bolt。

在JStorm的topology中，有两种组件：`spout`和`bolt`。

### spout

spout代表输入的数据源，这个数据源可以是任意的，比如说kafaka，DB，HBase，甚至是HDFS等，JStorm从这个数据源中不断地读取数据，然后发送到下游的bolt中进行处理。

### bolt

bolt代表处理逻辑，bolt收到消息之后，对消息做处理（即执行用户的业务逻辑），处理完以后，既可以将处理后的消息继续发送到下游的bolt，这样会形成一个处理流水线（pipeline，不过更精确的应该是个有向图）；也可以直接结束。

通常一个流水线的最后一个bolt，会做一些数据的存储工作，比如将实时计算出来的数据写入DB、HBase等，以供前台业务进行查询和展现。

## 组件的接口

JStorm框架对spout组件定义了一个接口：`nextTuple`，顾名思义，就是获取下一条消息。执行时，可以理解成JStorm框架会不停地调这个接口，以从数据源拉取数据并往bolt发送数据。

同时，bolt组件定义了一个接口：`execute`，这个接口就是用户用来处理业务逻辑的地方。

每一个topology，既可以有多个spout，代表同时从多个数据源接收消息，也可以多个bolt，来执行不同的业务逻辑。

## 调度和执行

接下来就是topology的调度和执行原理，对一个topology，JStorm最终会调度成一个或多个worker，每个worker即为一个真正的操作系统执行进程，分布到一个集群的一台或者多台机器上并行执行。

而每个worker中，又可以有多个task，分别代表一个执行线程。每个task就是上面提到的组件(component)的实现，要么是spout要么是bolt。

用户在提交一个topology的时候，会指定以下的一些执行参数：

### 总worker数

即总的进程数。举例来说，我提交一个topology，指定worker数为3，那么最后可能会有3个进程在执行。之所以是`可能`，是因为根据配置，JStorm有可能会添加内部的组件，如`__acker`或者`__topology_master`（这两个组件都是特殊的bolt），这样会导致最终执行的进程数大于用户指定的进程数。我们默认是如果用户设置的worker数小于10个，那么__topology_master 只是作为一个task存在，不独占worker；如果用户设置的worker数量大于等于10个，那么__topology_master作为一个task将独占一个worker

### 每个component的并行度

上面提到每个topology都可以包含多个spout和bolt，而每个spout和bolt都可以单独指定一个并行度(parallelism)，代表同时有多少个线程(task)来执行这个spout或bolt。

JStorm中，每一个执行线程都有一个task id，它从1开始递增，每一个component中的task id是连续的。

还是上面这个topology，它包含一个spout和一个bolt，spout的并行度为5，bolt并行度为10。那么我们最终会有15个线程来执行：5个spout执行线程，10个bolt执行线程。

这时spout的task id可能是1～5，bolt的task id可能是6～15，之所以是`可能`，是因为JStorm在调度的时候，并不保证task id一定是从spout开始，然后到bolt的。但是同一个component中的task id一定是连续的。

### 每个component之间的关系

即用户需要去指定一个特定的spout发出的数据应该由哪些bolt来处理，或者说一个中间的bolt，它发出的数据应该被下游哪些bolt处理。

还是以上面的topology为例，它们会分布在3个进程中。JStorm使用了一种均匀的调度算法，因此在执行的时候，你会看到，每个进程分别都各有5个线程在执行。当然，由于spout是5个线程，不能均匀地分配到3个进程中，会出现一个进程只有1个spout线程的情况；同样地，也会出现一个进程中有4个bolt线程的情况。

在一个topology的运行过程中，如果一个进程（worker）挂掉了，JStorm检测到之后，会不断尝试重启这个进程，这就是7*24小时不间断执行的概念。

## 消息的通信

上面提到，spout的消息会发送给特定的bolt，bolt也可以发送给其他的bolt，那这之间是如何通信的呢？

首先，从spout发送消息的时候，JStorm会计算出消息要发送的目标task id列表，然后看目标task id是在本进程中，还是其他进程中，如果是本进程中，那么就可以直接走进程内部通信（如直接将这个消息放入本进程中目标task的执行队列中）；如果是跨进程，那么JStorm会使用netty来将消息发送到目标task中。

## 实时计算结果输出
JStorm是7*24小时运行的，外部系统如果需要查询某个特定时间点的处理结果，并不会直接请求JStorm（当然，DRPC可以支持这种需求，但是性能并不是太好）。一般来说，在JStorm的spout或bolt中，都会有一个定时往外部存储写计算结果的逻辑，这样数据可以按照业务需求被实时或者近实时地存储起来，然后直接查询外部存储中的计算结果即可。



