---
title: JStorm 任务的动态伸缩
layout: plain_cn
top-nav-title: JStorm 任务的动态伸缩
top-nav-group: 进阶
top-nav-pos: 11
sub-nav-title: JStorm 任务的动态伸缩
sub-nav-group: 进阶
sub-nav-pos: 11
---
从Release 0.9.7开始，JStorm可以支持topology任务的动态调整(支持扩容和缩容，以及同时的扩容和缩容)，动态调整的过程中   topology任务不会被重启或者下线，保证服务的持续性。在系统高负载和低负载时都能够有效的利用机器资源。
动态调整包括Task和Worker两个维度。

```
- Task维度: Spout, Bolt，Acker并发数的动态调整
- Worker维度: Worker数的动态调整
```

Note: 动态伸缩当前仅对shuffer grouping有效(Shuffer, Localfirst, LocalOrShuffer)

---

## **动态伸缩的使用**

#### 1. 命令行方式

JStorm rebalance

```
   USAGE: jstorm rebalance [-r] TopologyName [DelayTime] [NewConfig]
   e.g. jstorm rebalance SequenceTest conf.yaml

   参数说明:
   -r: 对所有task做重新调度
   TopologyName: Topology任务的名字
   DelayTime: 开始执行动态调整的延迟时间
   NewConfig: Spout, Bolt, Acker, Worker的新配置(当前仅支持yaml格式的配置文件)
            
```

配置文件例子

```
   topology.workers: 4
   topology.acker.executors: 4

   topology.spout.parallelism:
     SequenceSpout : 2
   topology.bolt.parallelism:
     Total : 8
```

#### 2. API接口

```
   backtype.storm.command.rebalance:
   - public static void submitRebalance(String topologyName, RebalanceOptions options)
   - public static void submitRebalance(String topologyName, RebalanceOptions options, Map conf)
       Note: conf里带上zk信息，可以用来做对一个集群的远程提交
```

---

## **应用场景**

#### 1. 根据系统的负载情况，进行任务的动态调整

JStorm提供了task和worker维度一些系统监控负载情况查询的API。 用户可以通过这些API去判断当前任务的负载情况，由此去判断是否要对任务相关的component进行扩容或者缩容。当前提供的具体监控信息查询API如下

```
   backtype.storm.metric.TopologyMetrics

   Task:
      - 反序列队列负载: public static Map<Integer, Double> getTaskDeserializeQueueStatus(String topologyName, Map stormConf)
      - 执行队列负载: public static Map<Integer, Double> getTaskExecuteQueueStatus(String topologyName, Map stormConf)
      - 序列化队列负载: Map<Integer, Double> getTaskSerializeQueueStatus(String topologyName, Map stormConf)

   Worker:
      - CPU利用率: public static Map<Integer, Double> getWorkerCpu(String topologyName, Map stormConf)
      - Memory使用量: public static Map<Integer, Double> getWorkerMemory(String topologyName, Map stormConf)
```

#### 2. 集群扩容时，对任务的扩容

JStorm集群扩容时，可以对任务进行扩容，新的task会被分配在新的机器上。

---

## **常见问题**

#### 1. 系统资源的利用率问题

为了保证服务不中断，默认情况下，在扩容时会保证当前task, worker调度结果的不变。所以如果新的worker资源和task的扩容数，在和当前系统调度结果不匹配的情况下，会出现集群资源使用不均衡的问题。
比如:

``` 
   worker-1: task-1, task-2, task-3
   rebalance with newConf: workerNum=4, taskNum=4
   = > worker-1: task-1, task-2, task-3
       worker-2: task-4
```

在多次扩容后，如果出现了以上集群调度不均衡的情况。可用通过加参数"-r"去强制对所有的task做一次默认的均匀调度。

```
   e.g. jstorm rebalance -r SequenceTest
```