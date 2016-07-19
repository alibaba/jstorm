---
title: 和Storm编程方式区别
layout: plain_cn
top-nav-title: 和Storm编程方式区别
top-nav-group: 进阶
top-nav-pos: 2
sub-nav-title: 和Storm编程方式区别
sub-nav-group: 进阶
sub-nav-pos: 2
---
### 概叙

随着，JStorm 的规模越来越大，发现原有的很多Storm设计，只能适合小集群中运行，当集群规模超过100台时，均会出现一些或这或那的问题。

### 编程接口改变
> 当topology.max.spout.pending 设置不为1时（包括topology.max.spout.pending设置为null），spout内部将额外启动一个线程单独执行ack或fail操作， 从而nextTuple在单独一个线程中执行，因此允许在nextTuple中执行block动作，而原生的storm，nextTuple/ack/fail 都在一个线程中执行，当数据量不大时，nextTuple立即返回，而ack、fail同样也容易没有数据，进而导致CPU 大量空转，白白浪费CPU， 而在JStorm中， nextTuple可以以block方式获取数据，比如从disruptor中或BlockingQueue中获取数据，当没有数据时，直接block住，节省了大量CPU。

但因此带来一个问题， 处理ack/fail 和nextTuple时，必须小心线程安全性。

附属： 当topology.max.spout.pending为1时， 恢复为spout一个线程，即nextTuple/ack/fail 运行在一个线程中。