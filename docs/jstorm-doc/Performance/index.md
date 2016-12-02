---
title: Performance

layout: plain
---

* This will be replaced by the TOC
{:toc}

# 概述
本次测试主要测试JStorm/Apache Storm/Apache Flink/Heron吞吐量, 业界主流的几种流式计算框架。
从测试结果结果看，JStorm 大概是Apache Storm 4倍， Apache Flink 1.5 倍， Twitter Heron 2 ~ 10 倍。

# 性能背后原因

* Apache Storm, mini-batch 做的不够优秀， 导致效率并不高
* Apache Flink, mini-batch 做的非常优秀， 但资源利用率没有做上来， 因为单机所有的task都跑在一个进程内， 内存共享很充分，进程内通信也非常优秀，但单机下只在一个进程内部， 无法充分利用cpu 资源。也就是会遇到cpu 使用上限问题
* Twitter Heron, 存在致命设计缺陷
  * Heron 最大的噱头 “10 倍storm性能”， 但heron 对比的对象是storm 0.8.2,  这是3年前的storm，是上一代的storm， 而最新版storm 1.0.2 早已经是storm 0.8.2  的十倍性能。
  * heron在性能上存在几个致命缺陷
    * 失去了整个业界性能优化很大的一个方向， 流计算图优化。其核心思想就是让task尽量绑在一个进程中， 这样task之间的数据，可以直接走进程内通信，无需反序列化和序列化。
    * 为了提高稳定性， heron将每个task 独立成为一个进程， 则会产生一个新的问题，就是task之间的通信都不会有进程内通信， 所有task通信都是走网络， 都要经过序列化和反序列化， 引入了大量额外的计算. 
    *  如果想要图优化， 则heron必须引入一层新的概念， 将多个task 链接到一个进程中， 但这个设计和heron的架构设计理念会冲突
  * 每个container 的stream manager 会成为瓶颈， 一个container 内部的所有task 的数据（无论数据对外还是对内）通信都必须经过stream manager，  一个进程他的网络tps是有上限的， 而stream-manager的上限就是50w qps， 则表示一个container的内部通道和外部通道总和就是50w qps. 大家都必须抢这个资源。
  *  原来的一次网络通信， 现在会变成3次网络通信， task －》 当前container的streammanager －》 目标container的stream manager －》 目标task

# 测试环境
* 10 台a8, 32 核／128g 
* os: redhat rhel 7.2
* jdk: jdk8
* hdfs: 2.6.3
* mesos: 0.25.0
* aurora: 0.12.0
* jstorm: 2.2.0
* heron: 0.14.0 并打上heron最新性能优化patch
* flink: 1.0.3
* storm: 1.0.2
* worker/container 内存设置4g

# 使用FastWordCount

## 测试用例说明

FastWordCount 测试用例 来自[Apache Storm 的 FastWordCount](https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/org/apache/storm/starter/FastWordCountTopology.java) 分为3个stage, Spout 生成句子， 然后将句子发送到Split bolt, 而Split bolt 将句子切分成words， 然后对word 进行fieldGrouping (group by word), 最后 Count bolt 统计没个word的出现次数， spout-> split > count， 在该测试用例中，使用流计算中最常用的shuffler和fieldgrouping 方式， 所以基本能反映一个流式计算引擎的极限性能。

测试源码在https://github.com/alibaba/jstorm/tree/master/jstorm-utility/performance-test


## 测试结果
![screenshot](http://img1.tbcdn.cn/L1/461/1/b71f91209bf03ce76279c26e2200a1dea72c86ef)
![screenshot](http://img2.tbcdn.cn/L1/461/1/fc14b7cd6a6c427e8ff0c0939dc7196932973728)

## 测试插曲
*  第一次跑，heron性能极差， 10个container只有2w qps， 咨询twitter， 告知，大量触发反压，可以打个patch 调整反压策略
* 第二次跑（打上patch后），10个container可以到10多万， 但其实和storm相比，也是差很多, 咨询twitter 人员， 告知一个container内部task太多，容易发生反压
*  第三次跑（修改container内部task数）， 10个container 跑到20w qps， 但我们依旧不满意，咨询twitter， 他们告知 他们也就只能跑20w qps， 并且告诉 一个很大问题， container内部的stream－manager的瓶颈是50w qps， 也就是一个container所有内部通信和外部通信的总和上限就是50w。


# 使用WordCount

## 测试用例

WordCount 是twitter 号称是storm 10倍性能的[官方测试用例](https://github.com/twitter/heron/blob/master/heron/examples/src/java/com/twitter/heron/examples/WordCountTopology.java)， 分为2个stage， spout -> count， 使用shuffle模式，并按照twitter给出的并发配置进行测试。

## 测试结果

![screenshot](http://img3.tbcdn.cn/L1/461/1/a3adc55b13763d3f3651be2eb12484b832319e24)	
![screenshot](http://img3.tbcdn.cn/L1/461/1/73f9814ee8006537ae7bbd54f38edebf9ee03922)



# 真实用例 -- 营销产品账

## 测试用例

* 输入为TT，offset表/结果表为hbase，中间表为本机内存。
* 计算规则：12个group by条件下求max/min/count/sum等四个指标，每行记录共拆成48个指标。
* Topology分5个环节，一为Source，读取TT的数据，并记录offset；二为Mapper，解析数据及线上配置的规则，进行指标的分解；三为Reducer，增量内存计算，计算单位时间内的数据；四为Merge，合并计算，读取中间表数据，进行合并；五为Storage，持久化，写入最终结果表。

![test-logic]({{site.baseurl}}/img/performance/test-logic.png)

## 测试结果
![jstorm-heron.png]({{site.baseurl}}/img/performance/jstorm-heron.png)




