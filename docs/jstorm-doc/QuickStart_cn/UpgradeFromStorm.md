---
title:  "Migrate Application from Apache Storm to JStorm"
# Top-level navigation
top-nav-group: QuickStart_cn
top-nav-pos: 5
top-nav-title: 从Apache Storm升级到JStorm
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}




# 概叙

JStorm 是Apache Storm的超集，  JStorm 默认支持Storm  的所有客户端接口。 基于Apache Storm 0.9.5 版本的应用可以直接运行在JStorm 2.1.0或2.1.1 的集群中， 对于其他情况下， 则需要把应用的pom.xml依赖修改为jstorm的版本即可。


## 更改jstorm-core依赖：

```xml
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.1.1</version>
    <scope>provided</scope>
</dependency>
```
重新编译打包即可， 其中版本2.1.1 修改为jstorm集群的运行版本

## 注意事项：

### spout 多线程方式

一般情况下 spout 内部， 执行ack/fail 和nextTuple 是分别2个线程， 需要知晓，不是像storm一样运行在一个线程中。

当topology.max.spout.pending 设置不为1时（包括topology.max.spout.pending设置为null），spout内部将额外启动一个线程单独执行ack或fail操作， 从而nextTuple在单独一个线程中执行，因此允许在nextTuple中执行block动作，而原生的storm，nextTuple/ack/fail 都在一个线程中执行，当数据量不大时，nextTuple立即返回，而ack、fail同样也容易没有数据，进而导致CPU 大量空转，白白浪费CPU， 而在JStorm中， nextTuple可以以block方式获取数据，比如从disruptor中或BlockingQueue中获取数据，当没有数据时，直接block住，节省了大量CPU。

当topology.max.spout.pending为1时， 恢复为spout一个线程，即nextTuple/ack/fail 运行在一个线程中。
 
