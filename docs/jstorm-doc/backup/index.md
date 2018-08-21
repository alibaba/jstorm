---
title: Performance
layout: plain
---
# Preparation
* Server Number: 7,  nimbus/web ui/zookeeper run in one node,  supervisor run in the left 6 nodes
* Hardware: 
```
CPU: 24 Core(Intel(R) Xeon(R) CPU E5-2430 0 @ 2.20GHz)
Memory: 96G
Disk: 2T[sata,7200rpm]
Network Adapter: 1Ge*2
```
* Software:
```
OS: Redhat Enterprise Linux 5.7 x86_64 
Java: java version "1.6.0_32" 64bits
Storm: Storm-0.9.2-netty-p297
JStorm: jstorm-0.9.4.1 
```
* [JStorm testing source code](https://github.com/hustfxj/jstorm_test)
* [Storm testing source code](https://github.com/hustfxj/storm_test), JStorm testing source code is same as Storm's except the code read Zookeeper node, so there are two set of testing code. 

# Conclusion
1.  JStorm is faster than Storm
2. The ratio of task_num/worker_number is one key point, the performance isn't linearly growing as ratio growing. The ratio had better no bigger than 12 in JStorm, no bigger than 18 in Storm. Otherwise performance will drop down due to thread switch .
3. The bigger message size, the higher throughput, when CPU and network-throughput is enough
4. The simpler the topology, the higher throughput. 
5. Strongly suggest setting "topology.max.spout.pending", otherwise tuple is likely to occur failure.


# Performance test of changing worker number

### Test ###
Don't change executor/task parallelism, just set different worker number

### Setting ###
```
Spout number: 18
Bolt number: 18
Acker number: 18
Message Size: 10 bytes
Max.spout.pending: 10000
topology.performance.metrics: false
topology.alimonitor.metrics.post: false
disruptor.use.sleep: false
Topology Level: one kind of spout, one kind of bolt, shuffle grouping
```

## Test Result ##
![Throughput VS workers]({{site.baseurl}}/img/performance/throughput_workers.JPG)

Throughput VS Workers Remarks: The horizontal axis is the number of worker number, Y-coordinate is the emit speed of Spout every 10 seconds


![Cpu Usage VS workers]({{site.baseurl}}/img/performance/cpu_usage_workers.JPG)

Cpu Usage VS Workers

## Conclusion

1. JStorm performance is higher than storm's when task/worker number ratio is lower than 12;
2. JStorm save more cpu usage;
3. When worker number is 12, both JStorm throughput and Storm throughput is best, if worker number is bigger than 12, but the throughput reduce;
The root cause is as following:
1. The ratio of task number/worker number is bigger, the more data will be past in one JVM, no network cost, don't need do serialize/deserialize worker.
2. When the ratio of task number/worker number is much high, thread context switch will cost much CPU, so the throughput isn't simply increasing when enlarge the ratio of task/worker.
3. When server number is fixed, enlarge the worker's number, it will increase worker's common threads such as total dispatch thread, total sending thread, netty client thread, these thread will cost more cpu, so increasing worker's number doesn't simply improve the performance.


# Test Message Size 

## Test ##
Set different message size, check the throughput changing.

## Setting ##
```
Spout number: 18
Bolt number: 18
Acker number: 18
Worker number: 12
Max.spout.pending: 10000
topology.performance.metrics: false
topology.alimonitor.metrics.post: false
disruptor.use.sleep: false
Topology Level: one kind of spout, one kind of bolt, shuffle grouping
```

## Test Result
 
![Throughput VS Message Size]({{site.baseurl}}/img/performance/throughput_msg_size.JPG)


![Cpu Usage VS Message Size]({{site.baseurl}}/img/performance/cpu_usage_msg_size.JPG)

## Conclusion
1. Increasing message size will improve both JStorm and Storm throughput when CPU and network is enough
2. Normally JStorm throughput is higher than Storm's


# Test Topology with different bolt layer

## Test ##
Set different topology bolt layer, check the throughput changing.

## Setting ##
```
Spout number: 18
Bolt number: 18
Acker number: 18
Worker number: 12
Max.spout.pending: 10000
message-size=10 byte
topology.performance.metrics: false
topology.alimonitor.metrics.post: false
disruptor.use.sleep: false
Topology Level: one kind of spout, more kind of bolt, shuffle grouping
```

## Test Result ##
 
![Throughput VS bolt level]({{site.baseurl}}/img/performance/throughput_bolt_level.JPG)


![Cpu Usage VS bolt level]({{site.baseurl}}/img/performance/cpu_usage_msg_size.JPG)

## Conclusion ##
Increasing bolt level will reduce the throughput normally, increase cpu usage.