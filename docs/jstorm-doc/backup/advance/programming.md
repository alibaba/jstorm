---
title: Programming Experience
layout: plain
top-nav-title: Programming Experience
top-nav-group: advance
top-nav-pos: 8
sub-nav-title: Programming Experience
sub-nav-group: advance
sub-nav-pos: 8
---
*  In Storm, nextTuple and ack/fail are running in the same thread, where block/sleep operation is not allowed in these function, otherwise tuple is likely to timeout. On the contrary, when there is no input data in nextTuple, the whole spout is running in vain, which wastes CPU resources greatly. In JStorm, the function nextTuple and ack/fail in spout are running in different thread, which recommends spout does block operation(e.g., take messages from queue) or sleep operation in nexTuple when no data to fetch.  This design will save much CPU when there is no much input data for spout.

*   In the past 3 years, we have implemented tens of application basing JStorm, we strong recommend the architecture of Message Middleware + JStrom + External Storage as followed. ![Application-Solution]({{site.baseurl}}/img/advance/programming/Application-architecture.JPG)
Spout get data from the Message-oriented Middleware (eg. RocketMq or Kafka) and then compute the data, store the result to the external storage (eg. HBase, MySQL, Redis) finally, other system fetch data from the external storage, doesn't communicate with JStorm. Although JStorm provide DRPC framework, but this framework's performance is pretty pool and the other system need depend on JStorm's service, the architecture isn't clean.  At the same time, when the computing is wrong, just reset RocketMq/Kafka offset and restart JStorm Topology, then it will get a correct result. In BI/Data Warehourse, it is often rerun the topology due to computing logic changing, it is recommend that all data in Kafka/RocketMq contain timestamp, so it is easy to reset offset to one special time.

*   When using RocketMq/Kafka through listener method and add/restart Kafka/RocketMq Partition, it is need to restart topology, otherwise it is likely to failed to fetch some partition's data.

*   When adding Kafka/RocketMq partition, it had better the new partition's brokerId is bigger than the old ones', so the order of old partitions in spouts won't be changed and old partitions's consumer won't be changed too, this is important for Transaction/Trident topology.  

*   In normal mode, we should use IBasicBolt as much as possible to avoid missing call Collector.ack.

*   For RocketMQ/Kafka, the fetching data frequency should not be too high, when the interval is smaller than 100ms, it is easy to no data to fetch but waste much CPU.

*  A fetch block size had better be 2MB or 1MB, since too big block is likely to cause Java GC, and too small will lower the efficiency.

* It had better run 2 ~ 8 tasks in one worker, the more data to be handled in one task, the smaller the number. Running multiple tasks in one worker will save much CPU, more data will be past in one process, no network cost, no serialize/deserialize operation. But the number bigger than 12 isn't be suggested, due to thread switch maybe cost much CPU. 

* Topology had better add alert such as :
1. Not all yesterday's data have been sent to Hadoop till 2 AM.
2. The component's receve/send QPS is lower than one threshold value
3. The number of failure tuple is bigger than one threshold value
4. The CPU/Memory usage is bigger than one threshold value.



* From JStorm 0.9.5.1, JStorm provide 3 kinds of RPC frameworker, Netty Asynchronous mode, Netty Synchronous mode, ZeroMq Asynchronous mode. 
1. Netty Asynchronous mode, this is the default setting. Client just continuously send data to server, the performance is better, but if server handling data speed is slow, it is easy to fail.
2. Netty synchronous mode(storm.messaging.netty.sync.mode: true), client can't send data until receive one response of server, the performance is lower, the data flow is more stable. But the biggest problem is that the data flow maybe in deadblock in some special case, both send QPS and receive QPS are 0.
3. ZeroMQ  asynchronous mode, this isn't recommended to be used. it is like "Netty Asynchronous mode", but all data have been sent by ZeroMq.

* It had better use curator framework to access Zookeeper, but please use one stable version. Some version exist problem, we have met some problems in curator-recipes

* Anyone who have other experience, please feel free to update this wiki or send email to me  hustjackie@gmail.com.