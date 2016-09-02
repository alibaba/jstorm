---
title:  "JStorm 限流控制/反压"

#sub-nav-parent: AdvancedUsage
sub-nav-group: AdvancedUsage_cn
sub-nav-id: BackPressure_cn
sub-nav-pos: 9
sub-nav-title: 限流控制/反压
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}

# 背景

限流控制，又称 反压 （backpressure）， 这个概念现在在大数据中非常火爆， 尤其是最近Heron/Spark都实现了这个功能。其实在jstorm 0.9.0 时，底层netty的同步模式，即可做到限流控制， 即当接收端能处理多少tuple， 发送端才能发送多少tuple， 但随着大面积使用， 发现netty的同步模式会存在死锁问题， 故这种方式并没有被大量使用。

# 原理
后来自2015年6月，twitter发布了heron的一篇论文， 描叙了，当下游处理速度更不上上游发送速度时， 他们采取了一种暴力手段，立即停止spout的发送。
这种方式， jstorm拿过来进行压测， 发现存在大量问题， 当下游出现阻塞时， 上游停止发送， 下游消除阻塞后，上游又开闸放水，过了一会儿，下游又阻塞，上游又限流， 如此反复， 整个数据流一直处在一个颠簸状态。

真正合适的状态时， 上游降速到一个特定的值后， 下游的处理速度刚刚跟上上游的速度

## 什么样才能触发反压
jstorm的限流机制， 当下游bolt发生阻塞时， 并且阻塞task的比例超过某个比例时（现在默认设置为0.1）， 即假设一个component有100个并发，当这个component 超过10个task 发生阻塞时，才会触发启动反压限流

## 什么样的情况才能判断是阻塞
在jstorm 连续4次采样周期中采样，队列情况，当队列超过80%（可以设置）时，即可认为该task处在阻塞状态

## 触发谁限流
根据阻塞component，进行DAG 向上推算，直到推算到他的源头spout， 并将topology的一个状态位，设置为 “限流状态”

## 怎么限流
当task出现阻塞时，他会将自己的执行线程的执行时间， 传给topology master， 当触发阻塞后， topology master会把这个执行时间传给spout， 于是， spout每发送一个tuple，就会等待这个执行时间。storm 社区的人想通过动态调整max_pending达到这种效果，其实这种做法根本无效。

## 怎样解除限流
当spout降速后， 发送过阻塞命令的task 检查队列水位连续4次低于0.05时， 发送解除反应命令到topology master， topology master 发送提速命令给所有的spout， 于是spout 每发送一个tuple的等待时间－－， 当spout的等待时间降为0时， spout会不断发送“解除限速”命令给 topology master， 而topology master确定所有的降速的spout都发了解除限速命令时， 将topology状态设置为正常，标志真正解除限速


# 如何使用
```
## 反压总开关
topology.backpressure.enable: true
## 高水位 －－ 当队列使用量超过这个值时，认为阻塞
topology.backpressure.water.mark.high: 0.8
## 低水位 －－ 当队列使用量低于这个量时， 认为可以解除阻塞
topology.backpressure.water.mark.low: 0.05
## 阻塞比例 －－ 当阻塞task数／这个component并发 的比例高于这值时，触发反压
topology.backpressure.coordinator.trigger.ratio: 0.1

## 反压采样周期， 单位ms
topology.backpressure.check.interval: 1000
## 采样次数和采样比例， 即在连续4次采样中， 超过（不包含）（4 ＊0.75）次阻塞才能认为真正阻塞， 超过（不包含）(4 * 0.75)次解除阻塞才能认为是真正解除阻塞
topology.backpressure.trigger.sample.rate: 0.75
topology.backpressure.trigger.sample.number: 4
```

# 动态调整

```
jstorm update_topology topology-name -conf confpath
```
confpath 放置 上叙的配置