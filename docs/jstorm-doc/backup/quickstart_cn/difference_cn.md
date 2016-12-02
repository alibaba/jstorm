---
title: 和Storm的区别
layout: plain_cn
top-nav-title: 和Storm的区别
top-nav-group: 快速开始
top-nav-pos: 5
sub-nav-title: 和Storm的区别
sub-nav-group: 快速开始
sub-nav-pos: 5
---
JStorm VS Storm 请参看 [JStorm 0.9.0 介绍.pptx](http://wenku.baidu.com/view/59e81017dd36a32d7375818b.html)

JStorm 比Storm更稳定，更强大，更快， Storm上跑的程序，一行代码不变可以运行在JStorm上。


[Flume](http://flume.apache.org/) 是一个成熟的系统，主要focus在管道上，将数据从一个数据源传输到另外一个数据源， 系统提供大量现成的插件做管道作用。当然也可以做一些计算和分析，但插件的开发没有JStorm便捷和迅速。

[S4](http://incubator.apache.org/s4/) 就是一个半成品，健壮性还可以，但数据准确性较糟糕，无法保证数据不丢失，这个特性让S4 大受限制，也导致了S4开源很多年，但发展一直不是很迅速。

[AKKA](http://akka.io/) 是一个Actor模型，也是一个不错的系统，在这个Actor模型基本上，你想做任何事情都没有问题，但问题是你需要做更多的工作，Topology怎么生成，怎么序列化。数据怎么流（随机，还是group by）等等。

[Spark](http://spark.incubator.apache.org/) 是一个轻量的内存MR， 更偏重批量数据处理

