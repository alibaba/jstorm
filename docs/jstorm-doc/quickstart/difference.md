---
title: Difference between Storm and JStorm
layout: plain
top-nav-title: Difference between Storm and JStorm
top-nav-group: quickstart
top-nav-pos: 5
sub-nav-title: Difference between Storm and JStorm
sub-nav-group: quickstart
sub-nav-pos: 5
---
**Selection of JStorm/Storm/flume/S4**

JStorm vs Storm please click here([JStorm Introduction](http://42.121.19.155/jstorm/JStorm-introduce-en.pptx))

JStorm is more stable, more powerful, faster than Storm, and the old program running on Storm can run on JStorm without change one line of source code. It is a superset of Storm.


***


[Difference between JStorm and Storm](https://github.com/alibaba/jstorm/wiki/difference_jstorm_vs_storm)


***


[Flume](http://flume.apache.org/) is a mature system that focuses on data pipe, it can transfer data from one data source to another data source, and the system provides a large number of ready-made pipe plugin. Certainly you can also do some calculations and analysis, but the development of plugin is not convenient and fast as JStrom.

[S4](http://incubator.apache.org/s4/) is a semi-finished products, system robustness is OK, but poor data accuracy, can not guarantee the data fault-tolerant, this feature makes the S4 greatly restricted, is also the reason why few projects are using S4 since it is opened source.

[AKKA](http://akka.io/)is an actor model, is also a good system, based on the actor model, you can do anything whatever you want, but the problem is that you need to do everything, you should consider the topology how to generate, how to serialization, how data flows (random or group by), and so on.

[Spark](http://spark.incubator.apache.org/) is a lightweight memory MR, more focus on bulk data processing, it can handle higher throughput, but more latency.
