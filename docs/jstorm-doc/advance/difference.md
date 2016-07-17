---
title: Programming difference from Storm
layout: plain
top-nav-title: Programming difference from Storm
top-nav-group: advance
top-nav-pos: 2
sub-nav-title: Programming difference from Storm
sub-nav-group: advance
sub-nav-pos: 2
---
## Summary:
Generally, most of JStorm desgin is from Storm, and JStorm directly import Storm's client such as API, transaction, trident. Normally the Storm application can run in JStorm Cluster without changing java code, but there are still minor difference between JStorm and Storm from the application side. 
Why exist difference?
1. With the increase of JStorm Cluster, we found there are lots of design which isn't suitable for large scale cluster.
2. jstorm-client-extension provide lots of feature which are lack in Storm, such as classloader, user-define assignment, worker memory setting etc..

***

The programming interface changes as following:

* In Storm, nextTuple and ack/fail are running in the same thread, where block/sleep operation is not allowed in these function, otherwise tuple is likely to timeout. On the contrary, when there is no input data in nextTuple, the whole spout is running in vain, which wastes CPU resources greatly. In JStorm, the function nextTuple and ack/fail in spout are running in different thread, which recommends spout does block operation(e.g., take messages from queue) or sleep operation in nexTuple when no data to fetch. This design will save much CPU when there is no much input data for spout.
```
#Force spout use single thread
spout.single.thread: false
```
If this setting is true, it will force spout use only one thread, nextTuple/ack/fail will run in one thread. At the same time, if "topology.max.spout.pending" is 1, it will force spout use only one thread too.

* When the number of acker isn't 0, bolt should call Collector.ack(tuple) or Collector.fail(tuple) after execute one tuple. Because all metrics will be collected only when bolt execute Collector.ack or Collector.fail when acker isn't 0 whatever the tuple emitted from spout contains messageId or not.

* Please refer to [jstorm-cleint-extension](https://github.com/alibaba/jstorm/blob/master/jstorm-client-extension/src/main/java/com/alibaba/jstorm/client/ConfigExtension.java) for new features.
