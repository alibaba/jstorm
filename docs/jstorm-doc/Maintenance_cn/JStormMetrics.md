---
title:  "JStorm Metrics"
# Top-level navigation
top-nav-group: Maintenance_cn
top-nav-pos: 4
top-nav-title: JStorm Metrics
---

* This will be replaced by the TOC
{:toc}


# JStorm Metrics与Storm Metrics的比较

<table>
    <thead>
        <tr>
            <th>---</th>
            <th>Storm/stats</th>
            <th>Storm/built-in metrics</th>
            <th>JStorm metrics</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>窗口</td>
            <td>10m, 3h, 1d, all-time</td>
            <td>1m</td>
            <td>1m, 10m, 2h, 1d</td>
        </tr>
        <tr>
            <td>采样率</td>
            <td>5%, 所有metrics都会采样</td>
            <td>同stats</td>
            <td>10%, counter不采样（精确计算）, meters/histograms采样</td>
        </tr>
        <tr>
            <td>metric数据流</td>
            <td>executors/tasks发送至ZK</td>
            <td>executor发送至metrics consumer 至外部系统</td>
            <td>worker -> topology master -> nimbus -> 外部系统</td>
        </tr>
        <tr>
            <td>metrics数据</td>
            <td>k-v键值对</td>
            <td>stream/executor metrics, topology metrics在调用时计算</td>
            <td>预聚合的 metrics of stream/task/component/topology/cluster/worker/netty/nimbus metrics</td>
        </tr>
        <tr>
            <td>metrics值</td>
            <td>采样计算的counter, meters/histogram平均值</td>
            <td>同stats</td>
            <td>counter精确值，meter值：m1/m5/m15/mean, histogram值：p50/p75/p90/p95/p98/p99/p999/min/max/mean</td>
        </tr>
        <tr>
            <td>更新策略</td>
            <td>按照时间分桶，如果窗口大的话更新间隔很长</td>
            <td>每分钟</td>
            <td>所有窗口每分钟windows</td>
        </tr>
        <tr>
            <td>zk依赖</td>
            <td>数据写入zk</td>
            <td>N/A</td>
            <td>N/A</td>
        </tr>
    </tbody>
</table>


# JStorm Metrics 设计

## 设计目标

1. 能看到从流级别到集群级别的所有metrics，至少1分钟更新一次
2. 能看到metrics的历史值(曲线)
3. 支持常见metric类型以及更准确的metric统计
4. 支持topology历史, 通过topology历史可以查看历史metrics
5. 支持task轨迹: 当task挂的时候，可以看到task在集群中的整个迁移历史 
6. 支持插件形式替换metrics存储
7. 支持用户自定义metrics
8. 通过metrics简化问题排查

## 基础流程

```seq
worker->worker: 创建JStormMetricsReporter
worker->worker: JStormMetrics.registerMetrics注册至本地registry
worker->nimbus: JStormMetricsReporter.registerMetrics: 注册metrics meta
nimbus->nimbus: 保存metrics meta至StormMetricsCache
nimbus->external systems: 存储metrics meta至外部系统
nimbus->worker: 返回注册的metric meta: map&lt;metricId, metricName&gt;

worker->topology_master: JstormMetricsReporter向TopologyMaster发送metrics
topology_master->topology_master: 计算、聚合metrics
topology_master->nimbus: 发送metrics数据到nimbus
nimbus->nimbus: 保存metrics数据至JStormMetricsCache
nimbus->external systems: 发送metrics数据至外部MetricsUploader插件
```


## 基础概念

### metric类型
目前支持的metric类型
`counter/gauge/meter/histogram`

### meta类型
即metrics的聚合类型，目前支持以下几种类型的metrics： 
`stream/task/component/netty/worker/topology/cluster/nimbus`

通过topology/cluster metrics，我们就可以很容易地看出拓扑/集群整体的资源使用和水位情况(cpu，内存，网络，磁盘)。

### metric name
Metric names是一种结构化的，类似java package的名称，这与storm中的metrics完全不同。

对于 stream/task/component/netty类型的metrics, 一个metric name由以下部分组成： 
`metaType@metricType@topologyId@componentId@taskId@streamId@metricGroup@metricName`

当一个用户注册一个stream metric的时候，jstorm metrics框架就会自动注册task/component级别的metrics，并与stream metrics建立关联，以方便级联更新。

比如，当用户注册了一个`emitted`的counter类型的stream级别的metric，具体的：stream: default, taskId: 1, component: spout1, 
topologyId: SeqTest-1-1, 那么生成的metric name为：
`3@1@SeqTest-1-1@spout1@1@default@sys@emitted`
其中3是MetaType.STREAM的枚举值, 1是MetricType.COUNTER的枚举值。

然后，当调用了`registerStreamMetric(...)`方法之后, 对应的task/component metrics就会自动被注册，全名分别为： 
`3@1@SeqTest-1-1@spout1@1@@sys@emitted`
`3@1@SeqTest-1-1@spout1@@@sys@emitted`

非常简单，对于task metrics，我们只是将stream id置空，而对于component metrics，我们只是将stream id和task id同时置空。

对于topology级别的也是类型。因此，我们只提供了注册stream/task的方法，到component或者topology级别的聚合，内部自动实现。

对于worker/topology/cluster/nimbus级别的metrics, metric name由以下部分组成：
metaType@metricType@topologyId@host@port@metricGroup@metricName
(特殊地，对于cluster/nimbus, topologyId写死为`__CLUSTER__`/`__NIMBUS__`).

这种metric name的设计由于天然按key有序，可以很容易被一些类似于HBase的存储系统使用。

### metric id
当然metric name还是太长了，因此类似于opentsdb，我们拆分出了metric meta和metric data. 
metric meta其实就是metric id到metric name的一个映射，而metric data则是metric id到具体metric值的映射。 
目前JStorm使用GUID的低64位来作为metric id。

metric meta的机制的确引入了一些额外的复杂性，不过它能够节省大量空间。

## JStorm metrics中的重要模块（类）

### JStormMetrics
一个静态类，提供了`registerMetrics` 方法, 与codahale metrics类似, 所有的metrics都会存在于worker进程中的一个单例registry中。
这个类也负责自动的向上聚合注册。

### JStormMetricsReporter
这个类负责向nimbus注册metrics以及将worker/supervisor/nimbus的metrics数据发送至nimbus master或topology master。

需要注意的是，这个类可以存在于worker/supervisor以及nimbus。当它存在于worker的时候，它会向TM发送metrics，否则会直接向nimbus发送。

### TopologyMaster
负责聚合、计算所有worker的metrics数据（具体在`TopologyMetricContext`类中实现），聚合之后，它会通过thrift接口向nimbus发送metrics数据。

### ClusterMetricsRunnable
这个类位于nimbus中，它负责两件事情：
1. 生成metric id
2. 向外部系统发送metrics数据

### JStormMetricsCache
为了提高效率，我们引入了rocksdb作为nimbus以及nimbus端metrics的缓存。
实现上，所有来自topology master的metrics数据都会先被存储到rocksdb，然后等待被`MetricUploader`实现类处理。

如果用户没有实现`MetricUploader`接口，则默认会使用`DefaultMetricsUploader`，这个接口会从metric cache中读取数据，然后直接删除，
其他什么事情也不做。 
                                                                                    
使用rocksdb的另外一个原因是，如果将所有metrics数据都存在nimbus内存中，nimbus会有较大的GC压力，甚至可能会OOM。


## 其他

### 用户自定义metrics
我们提供了`MetricClient`来使用用户自定义metrics。
类似于`JStormMetrics.registerMetrics...` 方法, 当用户调用了`metricClient.registerGauge/Counter/Histogram`之后，所有的事情都
 交由JStorm来处理，包括metrics的聚合、汇总和计算。

### metrics数据的准确性
1. 为了提高histogram的准确性，我们并不是简单地把来自各个worker的histogram的分位数值取平均。实现上，我们不仅发送分位数值，还会发送采样的
分位点。然后会在topology master中通过采样点做二次计算。需要注意的是，这个功能如果开启，会消耗较多内存，因此默认是被关闭的。你可以通过
`topology.accurate.metric: true`来开启。
 
2. 跟storm不同的是，JStorm中的counter不会被采样，因此诸如`emitted`, `acked`, `failed`是精确的。
                        
3. 所有时间相关的metrics值的单位都是微秒，而不是毫秒。因为像`nextTuple_time`这样的值，如果用毫秒是没有意义的，它们通常都可能是0。
 
### topology历史和task事件
topology/task事件会被发送至`MetricUploader`，因此用户可以通过这个接口将此类事件存储至外部系统中。

### 构建metrics的监控系统
我们通过`MetricUploader`接口来实现写metrics来实现监控系统。有使用HBase和aliyun OTS的两种实现。


## 使用JStorm metrics 

### Metric配置
以下为metric配置项以及对应的说明。

#### topology.enable.metrics
全局的metric开关，一般仅用于测试，生产中**强烈不建议**关掉。

#### topology.metric.sample.rate
metric采样率，默认是0.1(10%)。注意这个值仅对histogram有效，因为counter和gauge无需采样。而meter使用了codahale meter内部的衰减机制，
也没有使用采样率。

#### topology.enable.metric.debug
与配置项`topology.debug.metric.names`一起使用，如果为true，它将会在JStormMetricsReporter中打印包含指定name的数据。
注意这里的name为短metric name。

#### topology.debug.metric.names
调试的metric名称列表，通过逗号分隔，如'SendTps, RecvTps'

#### topology.disabled.metric.names
在`JStormMetricsReporter.updateMetricConfig`方法中使用，禁用的metric名称（默认全部都启用），逗号分隔，这个配置项的值会动态更新，只要
storm.yaml值有更新。如果要动态禁用特定metrics，可以通过修改storm.yaml中这个配置项来实现。

#### topology.enabled.metric.names
在`JStormMetricsReporter.updateMetricConfig`方法中使用，启用的metric名称（默认全部都启用），逗号分隔，这个配置项的值会动态更新，只要
storm.yaml值有更新。如果要动态启用特定metrics，可以通过修改storm.yaml中这个配置项来实现。

#### nimbus.metric.uploader.class
这个配置项仅对nimbus master生效，即实现了`MetricUploader`的插件类。

#### nimbus.metric.query.client.class
这个配置项仅对nimbus master生效，实现了`MetricQueryClient`的插件类，这个类用于读取metric元数据及数据。

#### topology.max.worker.num.for.netty.metrics
由于netty metrics是基于每个连接进行注册的，因此当拓扑非常大的时候，会导致netty metrics的量特别大，而这些数据都会发送至topology master
以及nimbus，会给tm和nimbus都带来很大的压力。我们设置了一个硬编码的上限：当一个拓扑的worker数大于200的时候，netty metrics就会直接被禁用。
不过用户仍然可以默认把netty metrics禁用掉，将它的值设置为1即可禁用。

## 关于Metric uploader & metric query client
上面的配置项中已经说明了`nimbus.metric.uploader.class` 和 `nimbus.metric.query.client.class`的用途。
在我们的实现经验上，使用一个高效、快速的metric uploader以及稳定可靠的外部存储都非常重要。如果想要基于JStorm metrics构建监控系统，
我们推荐使用HBase来作为metrics的存储。

metric uploader在实现时还需要考虑的一个问题是nimbus的GC。对于一个大规模的集群，nimbus中可能会有非常多的metrics对象，为了缓解nimbus的
压力，我们目前的做法是先将metrics数据存到rocksdb中，然后在metric uploader中按需取出数据写入到外部存储中。

因此这里有实现上的一点提示，假设你使用了一个线程池来往HBase写入数据。那么不推荐的是下面的做法：

> 1. 创建一个Runnable对象
> 2. 到rocksdb中获取metrics数据，然后放入Runnable对象中
> 3. 将这个Runnable放入线程池的队列中等待执行

这个实现的主要问题是，如果你的线程池写入不够快，会导致在内存中堆积太多的对象。推荐的做法如下：

> 1. 创建一个Runnable对象 
> 2. 获取metric数据的key/index，并放入Runnable对象中。注意此时并没有取真正的数据。
> 3. 将这个Runnable放入线程池的队列中等待执行

因为metrics数据是真正在线程执行的时候才会被获取，因此可以极大缓解nimbus的GC压力。


## 使用MetricClient
JStorm提供了`MetricClient`，因此用户可以很容易地使用自定义metrics。具体使用示例如下：

1. 定义metric client对象

```
private MetricClient metricClient;
```

2. 在bolt.prepare或spout.open中，初始化该对象

```
metricClient = new MetricClient(context);

Gauge<Double> gauge = new Gauge<Double>() {
    private Random random = new Random();

    @Override
    public Double getValue() {
        return random.nextDouble();
    }

};
myGauge = metricClient.registerGauge("myGauge", gauge);
myCounter = metricClient.registerCounter("myCounter");
myMeter = metricClient.registerMeter("myMeter");
myHistogram = metricClient.registerHistogram("myHistogram");
```

仅此而已！你可以在你的方法中调用`myCounter.update`或`myMeter.update`等方法，我们就会为你做剩下的事情！

完整的API请参考`MetricClient`类


## TODO
1. 简化发往nimbus的metrics数据


## 附录一: Metrics含义

### Topology Metrics

#### MemoryUsed
cluster/topology/worker使用到的物理内存

#### HeapMemory
cluster/topology/worker JVM使用到的堆内存

#### CpuUsedRatio 
cluster/topology/worker cpu利用率，62.000 表示使用0.62个cpu，200.00表示使用2个cpu

#### NettyCliSendSpeed
cluster/topology/worker当前发送流量,单位字节/每秒

#### NettySrvRecvSpeed
cluster/topology/worker当前接收流量,单位字节/每秒

#### FullGc
cluster/topology/worker当前1分钟 full gc 次数

#### RecvTps
cluster/topology/component/task/stream 接收到的tuple的tps。

#### SendTps
cluster/topology/component/task/stream 发送tuple的tps。

#### Emitted
cluster/topology/component/task/stream 当前1分钟发送的消息数，包括业务消息和acker消息。

#### Acked
cluster/topology/component/task/stream 当前1分钟被ack的消息数。注意这个和Emitted的区别：
如果打开了acker机制， emitted的消息里面含有acker消息， 经常emitted 消息数量是acker消息数量的2倍。

#### Failed
cluster/topology/component/task/stream 当前1分钟 被ack失败的消息数（可能是没有完全处理，也可能是超时）。


### Component 级别

#### EmitTime
component/task/stream, 这是spout/bolt将消息发布到disruptor队列中的时间，单位为微秒，
JStorm从2.1.0开始所有时间相关的单位均为微秒。

#### DeserializeTime
component/task/stream, TaskReceiver中对一个tuple做反序列化的时间，单位为微秒。

#### SerializeTime
component/task/stream, TaskTransfer中对一个tuple做序列化的时间，单位为微秒。

#### ExecutorTime
component/task/stream, 只在spout中存在，nextTuple所花费的时间，单位为微秒。

#### ProcessLatency
component/task/stream, 这个是bolt execute消耗的时间，单位为微秒，
具体来说，就是从processTuple时，tuple被放进pending map时会给一个时间，
到调用ack的时候从pending map中取出来，用当前时间减去放入的时间，即为ProcessLatency。

如果是spout，则为从消息最初从spout发出，一直到最后收到acker的ack消息的完整时间。
在spout中，由于ProcessLatency意味着一个tuple走完了所有的bolt最后被ack，
因此通常会比较大（一般会比TupleLifeCycle还要大）。

#### TupleLifeCycle
component/task/stream, 这个是一个tuple或者一个batch从上一级component中被emit出来，单位为微秒，
到当前component接收到这个tuple或者batch的时间，这段时间包括了上游序列化时间、网络发送和下游反序列化时间的总和


### Task 级别

#### DeserializeQueue
反序列化队列堆积情况。补充说明，一个task 有4个队列， 反序列化队列，执行队列，控制消息队列，序列化队列。

#### SerializeQueue
序列化队列堆积情况。补充说明，一个task 有4个队列， 反序列化队列，执行队列，控制消息队列，序列化队列。

#### ExecutorQueue
执行队列堆积情况。补充说明，一个task 有4个队列， 反序列化队列，执行队列，控制消息队列，序列化队列。

#### CtrlQueue
控制执行队列的堆积情况。补充说明，一个task 有4个队列， 反序列化队列，执行队列，控制消息队列，序列化队列。

#### PendingNum
只对spout有效，表示 spout 中已经发送了但还没有ack的tuple数量

#### BatchInterval
性能调优使用， 表示2次batch打满时，间隔微秒


### Worker 级别

#### GCCount
当前1分钟gc的次数

#### GCTime
当前1分钟gc所花费的时间之和，单位是微妙

#### NettyCliSendBatchSize
当前1分钟worker 发送netty包的平均大小(Bytes)

#### NettySrvTransmitTime
当前1分钟，worker 解析netty包的耗时，单位微秒。

#### RecvCtrlQueue
worker级别的总接受控制队列堆积情况

#### SendCtrlQueue
worker级别的总发送控制队列堆积情况

### supervisor 级别

#### DiskUsage
当前jstorm账户所在文件磁盘空间的利用率；

#### MemoryUsage
当前机器的内存利用率

#### CpuUsedRatio
当前机器的cpu利用率

#### NettyCliSendSpeed/NettySrvRecvSpeed
当前机器网卡每秒接收和发送字节数
