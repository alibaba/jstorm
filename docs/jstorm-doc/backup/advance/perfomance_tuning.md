---
title: Performance Tuning
layout: plain
top-nav-title: Performance Tuning
top-nav-group: advance
top-nav-pos: 10
sub-nav-title: Performance Tuning
sub-nav-group: advance
sub-nav-pos: 10
---
Before reading this page, please read [Operation Experience]({{site.baseurl}}/advance/operation) and [Programming Experience]({{site.baseurl}}/advance/programming)


# First decision
Before design a application, user must know the use scenario of ackerless/acker/trident frameworker. From performance side, the performance sequence is as following: ackerless > acker > trident.  Normally performance isn't the key point, developer must do tradeoff from performance/stability/scalability.

# Find out which component's throughput is least
All tuples of topology in JStorm is being past in one message pipeline, the whole topology's throughput will be decided by the lowest component's throughput, so find out the bottleneck of topology is a key point.
Here list one example to find out which component's throughput is least in JStorm 0.9.6.1.

SpoutA -> BoltA -> acker

SpoutA's max throughpout is (parallelism * (1000/"NextTuple: Time(ms)"))

BoltA's max throughpout is (parallel * (1000/max("Deserialize: Time(ms)", "Execute: Time(ms)", "Serialize: Time(ms)")))

Acker's max throughpout is (parallel * (1000/max("Deserialize: Time(ms)", "Execute: Time(ms)", "Serialize: Time(ms)"))) 

In this method, we can find out which component's throughpout is the bottleneck of topology, so the simplest improving throughpout's method is enlarge the bottleneck component's parallelism.  

Another easy solution to find out bottleneck component is finding out whose queues are full, if most of one component's queues are full, please enlarge the component's parallelism.

# Hotpoint task
When using fieldGrouping, please take care of hotpoint task which receive more data than other tasks belonging to the same component, this will lower down the whole topology's throughput greatly. Please make task load as equal as possible.


# Hotpoint worker
For some heavy component, please make sure every worker's loader is almost equal, so the parallelism of the component had better be multiple of the worker number.

#Hotpoint/Bad Supervisor
Please pay attention to this problem When whole cluster's load is heavy. Here are several solution to resolve this problem:

1. let the worker number is multiple of the supervisor number and reduce hotpoint  worker
2. Do connection check after install one cluster, You can use the installation package's example to do this check.
3. Check supervisor's page, check whether there are some workers whose one of CPU usage/"Batch Trans Time over network(ms)"/"Netty Client Batch Size" is pretty high

#Hotpoint spout
Normally spout consumer Kafka/RockeMq/TimeTunel/Redis, the data source's partition number had better be the multiple of spout's number.

# Worker number
It had better run 2 ~ 8 tasks in one worker, the more data to be handled in one task, the smaller the number. Running multiple tasks in one worker will save much CPU, more data will be past in one process, no network cost, no serialize/deserialize operation. But the number bigger than 12 isn't be suggested, due to thread switch maybe cost much CPU.

#Some JStorm setting for performance

```
topology.max.spout.pending: null                 #this will effect spout emit tuple number in one time, the bigger it is, the more tuple it will emit, the easier occur failure
topology.message.timeout.secs: 30                 #the tuple's timeout seconds
topology.executor.receive.buffer.size: 256        #the task's receiving queue size
topology.executor.send.buffer.size: 256           #the task's sending queue size
topology.transfer.buffer.size: 1024               # the worker's sending/receive queue size, shared by all tasks
kryo.enable: true                                 # if this is true, it will reduce deserialize time, but the value of emit tuple should register into kryo factory.

## network settings
#storm.messaging.transport: "com.alibaba.jstorm.message.zeroMq.MQContext"
storm.messaging.transport: "com.alibaba.jstorm.message.netty.NettyContext"
storm.messaging.netty.sync.mode: false            ## send message with sync or async mode
storm.messaging.netty.transfer.async.batch: true  # If async and batch is used in netty transfer, netty will batch message
# If async and batch is used in netty transfer, netty will batch message
storm.messaging.netty.transfer.async.batch: true
# If storm.messaging.netty.transfer.async.batch is enable, the batch size.
storm.messaging.netty.transfer.batch.size: 262144
# every "storm.messaging.netty.flush.check.interval.ms" ms, netty will flush the batch.
storm.messaging.netty.flush.check.interval.ms: 10

```