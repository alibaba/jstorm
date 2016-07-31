---
title:  "How to do exactly-once or transaction?"

layout: plain_cn

sub-nav-group: Transaction_cn
sub-nav-id: OldTransaction_cn
sub-nav-pos: 2
sub-nav-title: 老事务(性能差)
---

* This will be replaced by the TOC
{:toc}

# 概述

老的storm的事务主要用于对数据准确性要求非常高的环境中，尤其是在计算交易金额或笔数，数据库同步的场景中。

老的storm 事务逻辑(需要使用trident)是挺复杂的，而且坦白讲，代码写的挺烂的。 JStorm的新事务请参考[JStorm新事务]({{site.baseurl}}/ProgrammingGuide_cn/Transaction/)


## Storm 事务的核心设计思想：

Transaction 还是基于api的属性之上，做的一层封装，从而满足transaction

其实，相当于把一个batch当做一个原子tuple来处理，只是中间计算的过程，可以并发。

### 核心设计1
提供一个strong order，也就是，如果一个tuple没有被完整的处理完，就不会处理下一个tuple，说简单一些，就是，采用同步方式。并对每一个tuple赋予一个transaction ID，这个transaction ID是递增属性（强顺序性），如果每个bolt在处理tuple时，记录了上次的tupleID，这样即使在failure replay时能保证处理仅且处理一次

### 核心设计2
如果一次处理一个tuple，性能不够好，可以考虑，一次处理一批（batch tuples）
这个时候，一个batch为一个transaction处理单元，当一个batch处理完毕，才能处理下一个batch，每个batch赋予一个transaction ID。

### 核心思想3
如果在计算任务中，并不是所有步骤需要强顺序性，因此将一个计算任务拆分为2个阶段：
1.    processing 阶段：这个阶段可以并发
2.    commit阶段：这个阶段必须强顺序性，因此，一个时刻，只要一个batch在被处理
任何一个阶段发生错误，都会完整重发batch

### 结果
一次性从Meta或Kafka 中取出一批数据，然后一条一条将数据发送出去，当所有数据均被正确处理后， 触发一个commit 流，这个commit流是严格排序，通常在commit流中进行flush动作或刷数据库动作，如果commit流最后返回也成功，spout 就更新Meta或kafka的偏移量，否则，任何一个环节出错，都不会更新偏移量，也就最终重复消费这批数据。


### 具体代码逻辑
代码逻辑参考下图

![transaction]({{site.baseurl}}/img/programguide/storm.transaction.jpg)
