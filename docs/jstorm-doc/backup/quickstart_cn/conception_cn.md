---
title: 基本概念
layout: plain_cn
top-nav-title: 基本概念
top-nav-group: 快速开始
top-nav-pos: 2
sub-nav-title: 基本概念
sub-nav-group: 快速开始
sub-nav-pos: 2
---
# 流
在JStorm中有对于流stream的抽象，流是一个不间断的无界的连续tuple，注意JStorm在建模事件流时，把流中的事件抽象为tuple即元组，后面会解释JStorm中如何使用tuple。

![STREAM]({{site.baseurl}}/img/quickstart_cn/conception/stream.jpg)

# Spout/Bolt
JStorm认为每个stream都有一个stream源，也就是原始元组的源头，所以它将这个源头抽象为spout，spout可能是连接消息中间件（如MetaQ， Kafka， TBNotify等），并不断发出消息，也可能是从某个队列中不断读取队列元素并装配为tuple发射。

有了源头即spout也就是有了stream，那么该如何处理stream内的tuple呢，同样的思想JStorm将tuple的中间处理过程抽象为Bolt，bolt可以消费任意数量的输入流，只要将流方向导向该bolt，同时它也可以发送新的流给其他bolt使用，这样一来，只要打开特定的spout（管口）再将spout中流出的tuple导向特定的bolt，然后bolt对导入的流做处理后再导向其他bolt或者目的地。

我们可以认为spout就是一个一个的水龙头，并且每个水龙头里流出的水是不同的，我们想拿到哪种水就拧开哪个水龙头，然后使用管道将水龙头的水导向到一个水处理器（bolt），水处理器处理后再使用管道导向另一个处理器或者存入容器中。

![spoutbolt]({{site.baseurl}}/img/quickstart_cn/conception/spoutbolt.jpg)

# Topology
![topology]({{site.baseurl}}/img/quickstart_cn/conception/topology.jpg)

对应上文的介绍，我们可以很容易的理解这幅图，这是一张有向无环图，JStorm将这个图抽象为Topology即拓扑（的确，拓扑结构是有向无环的），拓扑是Jstorm中最高层次的一个抽象概念，它可以被提交到Jstorm集群执行，一个拓扑就是一个数据流转换图，图中每个节点是一个spout或者bolt，图中的边表示bolt订阅了哪些流，当spout或者bolt发送元组到流时，它就发送元组到每个订阅了该流的bolt（这就意味着不需要我们手工拉管道，只要预先订阅，spout就会将流发到适当bolt上）。
插个位置说下Jstorm的topology实现，为了做实时计算，我们需要设计一个拓扑图，并实现其中的Bolt处理细节，JStorm中拓扑定义仅仅是一些Thrift结构体，这样一来我们就可以使用其他语言来创建和提交拓扑。

# Tuple
JStorm将流中数据抽象为tuple，一个tuple就是一个值列表value list，list中的每个value都有一个name，并且该value可以是基本类型，字符类型，字节数组等，当然也可以是其他可序列化的类型。拓扑的每个节点都要说明它所发射出的元组的字段的name，其他节点只需要订阅该name就可以接收处理。

# Worker/Task
Worker和Task是JStorm中任务的执行单元， 一个worker表示一个进程，一个task表示一个线程， 一个worker可以运行多个task。

 backtype.storm.Config.setNumWorkers(int workers)是设置worker数目，表示这个Topology运行在多个个jvm（一个jvm是一个进程，即一个worker）；backtype.storm.topology.TopologyBuilder.setSpout(String id, IRichSpout spout,	Number parallelism_hint)和setBolt(String id, IRichBolt bolt,Number parallelism_hint)中的参数
parallelism_hint表示这个spout或bolt有多少个实例，即对应多少个线程执行，一个实例对应一个线程。

# 资源slot(deprecated after 0.9.6.3)
在JStorm中，资源类型分为4种， CPU, Memory，Disk， Port， 不再局限于Storm的port。 即一个supervisor可以提供多少个CPU slot，多少个Memory slot， 多少个Disk slot， 多少个Port slot

*  一个worker就消耗一个Port slot， 默认一个task会消耗一个CPU slot和一个Memory slot
* 当task执行任务较重时，可以申请更多的CPU slot， 
* 当task需要更多内存时，可以申请更多的内存slot， 
* 当task 磁盘读写较多时，可以申请磁盘slot，则该磁盘slot给该task独享