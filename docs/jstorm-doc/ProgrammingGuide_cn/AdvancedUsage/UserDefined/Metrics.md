---
title: "User Defined Metrics, Monitor application's key Metrics"
layout: plain_cn

# Sub navigation
sub-nav-parent: UserDefined_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Metrics_cn
#sub-nav-pos: 2
sub-nav-title:  自定义监控
---

* This will be replaced by the TOC
{:toc}


# JStorm2.1.1中使用自定义metrics

## alimonitor client
2.x中，AlimonitorClient不再支持，因为koala中已经支持metrics的曲线展示，没有必要再把数据发送到alimonitor中。

## 自定义metrics类型

使用MetricClient来注册自定义metrics。目前支持以下几种metrics，请按需使用：

    COUNTER：计数器，如jstorm中的emitted, acked, failed都使用counter。

    METER：一般用于qps/tps的统计。jstorm中的sendTps, recvTps使用meter。

    GAUGE：一般用于统计一个瞬时值，如memUsed, cpuRatio，queueSize等这种瞬间的值。**但是需要注意，gauge是不可聚合的**，即如果你注册了一个task级别的gauge，你只能到koala web ui的task页面查看它的metrics，它并不会聚合到component级别，因为这种聚合没有实际意义（想象一下多个task的queueSize加一起是什么意思...）。

    HISTOGRAM：一般用于统计操作的耗时，如EmitTime，nextTupleTime等。


## 参考代码
http://gitlab.alibaba-inc.com/weiyue.wy/jstorm-sequence-test/blob/seq-test-for-asm-merge/src/main/java/com/alipay/dw/jstorm/example/sequence/bolt/TotalCount.java

## 在koala中配置监控报警
参见：http://www.atatech.org/articles/49312

## FAQ
Q：我想根据worker日志中的异常来添加监控报警，可以么？

A：不可以。jstorm不会去扫描日志的内容，因此无法做到。但是应用使用自定义metrics来间接做到这一点：首先在应用代码中添加一个自定义metrics，如counter，代表异常个数。然后在代码中捕获异常，给counter+1。最后只需要在koala中对这个自定义metrics配置监控报警即可。


---

# koala监控系统设计

## 监控层次

分为三层：

### 集群监控
如supervisor挂掉，supervisor变化（**注：暂未实现**）

### nimbus监控
考虑到在merge版本中，nimbus责任比较重要，单独把这个抽出来。主要有memory, FGC, OTS相关的参数（tps, insert）等。
**注：当前只实现了nimbus挂掉的监控报警**

### topology监控

主要包括：

a. topology挂掉。

b. topology状态变化，主要是从ACTIVE变为INACTIVE。

c. 有task长时间处于starting状态。

d. 有task频繁挂掉。

e. metric基线。根据不同的metric类型，可以选择不同的基线值。如meter可选m1, m5, m15，histogram可选p50, p75, p95, p98等。

## 监控项设计

### 监控表达式：
a.&gt;, &lt;, &ge;, &le;, !=, ==

b.波动：上升x%，下降x%，振动（包括了上升和下降）

且重复出现X次

### 报警级别
critical：如topology挂掉， nimbus挂掉

warning：如metrics值异常

对于critical的可以考虑用不同的通知方式，如电话等。warning只发短信、旺旺消息等。

**注：暂时未实现**


### 报警周期
7X24，上班时间，下班时间，等

1. 目前上午9：00～下午18：00，报警方式为旺旺报警，其他时间为短信报警。

2. 发生一次报警之后，下一次报警的时间至少为10分钟以后。如果半小时内发生了2次报警（相当于报警了之后，你都没有做处理），下一次报警的时间为1小时之后。

3. 用户可以控制报警的时间，可以选择关闭报警一段时间，或者完全关闭报警功能（koala中配置）

## 监控项管理

### 1.存储：监控项配置使用MySQL存储，监控报警数据使用OTS/HBase存储。

全集群1W个topology，每个配置50个监控项，也只有50W数据

有两种监控项，一种是topology状态监控，另外一种是metrics基线监控。用monitorType来标志：分别为S(status)和M(metrics)。

#### a.topology状态监控

主要是4种：

1)topology状态监控：状态从active转到其他状态时报警。

2)task starting：有task三分钟以上处于starting状态

3)task dead：task挂掉

4)queue full：有执行队列满的情况。


目前是存在topology_owner表中，添加了monitor_enable, topology_status, task_starting, task_dead, task_full字段。通过last_send_time, next_send_time和last_times做疲劳度控制。

~~key: clusterName + S + topologyName~~

~~value: 枚举值TOPOLOGY_STATUS, TASK_STARTING(3分钟以上), TASK_DEAD, TASK_QUEUE_FULL~~


#### b.metrics基线监控

~~key: clusterName + M + topologyName + metricId~~

~~value: metaType + metricType + valueField + expr + notifyLevel + notifyType + notifyPeriod + notifyList~~

根据topologyName取得所有监控项，遍历监控项，如果有效，则通过OTS/HBase scan过去10分钟的数据，对表达式进行求值对比。若满足报警条件，则加入报警队列（这里的QPS可能会比较高，预估会有1W+，不过都是小的scan）。

报警：需要根据报警周期判断用何种方式发送报警信息。并且，报警信息根据监控层次汇总，比如某个topology下有非常多的metric异常，汇总成一条报警信息，以减少对用户的干扰。

报警历史保存在MySQL中，方便做监控报警大盘分析和疲劳度控制。

当最新一次检测失败，需要发送报警的时候，先会从DB中取出过去3次发送的时间，加入疲劳控制逻辑，以决定是否发送报警消息。


### 一键添加所有相似的监控项

如Emitted, SendTps（不过这种情况下表达式会受到限制，只能使用波动表达式）

需要注意控制绝对值，如果绝对值很低，用波动表达式会产生误报。

**注：未实现**


### 禁用、添加、删除监控项

后台需要有一个用户的监控项配置页面。按照topology级别进行汇总。


### 监控荐允许继承

Emitted, SendTps等做成基础监控项。可以一键启用。
**注：未实现**


# 详细技术方案

## 监控项管理

koala页面中实现，存储在MySQL中。见上面的监控项管理。

## 报警实现

通过jstorm topology实现。

### ClusterSpout
1.读取koala的后台DB，拉到所有的集群列表。并逐一emit至DispatchBolt。

### DispatchBolt
1.循环所有的集群，读取该集群的topology列表，将每一个topology emit到CheckStatBolt。

### CheckStatBolt
1.从OTS中获取该topology的监控项，如果为空，直接结束，否则到2。

2.如果监控项不为空，则循环所有监控项，emit到报警计算bolt中。

a. topology挂掉。

b. topology状态变化，主要是从ACTIVE变为INACTIVE。

c. 有task长时间处于starting状态。

d. 有task频繁挂掉。

e. metric基线。根据不同的metric类型，可以选择不同的基线值。如meter可选m1, m5, m15，histogram可选p50, p75, p95, p98等。


按照上面的顺序，只要有一个条件满足，就不会执行下游的判断。即如果task状态不正常，就会直接发送报警，而不会再检查metrics数据是否正常。


### NotifyBolt
直接发送报警。
