---
title:  "How to do exactly-once or transaction"
# Top-level navigation
top-nav-group: ProgrammingGuide_cn
top-nav-pos: 3
top-nav-id: Transaction_cn
top-nav-title: 事务
layout: plain_cn

sub-nav-group: Transaction_cn
sub-nav-id: Transaction_cn
sub-nav-pos: 1
sub-nav-title: 新事务(性能好)
---

* This will be replaced by the TOC
{:toc}

# 概述

在原有的Storm设计中，Trident支持了只处理一次，Acker支持至少处理一次场景。但是Trident和Acker在这两种消息保证机制中，都面临着同样的性能问题， 如果用户需要exactly-once 保证， 性能会急剧下降。

同时由于Trident和Acker机制的API完全不同，用户很难用一套代码去支持两个场景。

针对这两个问题，我们对开始考虑做一套新的框架来支持者两个消息保证机制。

#  **基本原理:**  
设计参考了Flink的只处理一次方案，barrier和stream  align(流对齐)的设计，去做batch的划分和如何保证每个节点只会处理当前批次的消息。具体可以参考Flink的官方文档。
<u>https://ci.apache.org/projects/flink/flink-docs-release-1.0/internals/stream_checkpointing.html</u>
对于barrier和流对齐，本文不详细展开，主要介绍JStorm的实现和相关接口的使用。

## **基本介绍**

*  **状态维护:**   JStorm把topology任务的节点分为了四类节点，数据源spout，状态无关节点non-stateful  bolt，状态节点stateful  bolt，结束节点end  bolt。
  - *  数据源节点和状态节点只维护自己节点的状态提交顺序，在处理完每个batch后，会马上处理下一个batch的消息。
  - *  Topology  master负责每个batch的全局状态维护，既是否所有节点都完成了当前batch的处理。如果完成，topology  master开始做全局snapshot状态的持久化存储。
*  **如何回滚:**
  - * Barrier和stream  align让每个节点在做checkpoint时，保证batch的顺序性和一致性，而topology  master维护了全局状态。如果失败topology  master会通知所有状态节点  **回滚到最后一次全局成功的checkpoint**，然后数据源开始从最后一次全局成功的位置开始重新拉取消息。
  - * JStorm里回滚以数据源的种类为最小单位。比如说我们现在有两个spout  component，TT  spout和metaq  spout。如果metaq  spout的下游节点挂了，我们只会回滚，metaq  spout这个流的数据。


##  **接口介绍**

**Spout:  ITransactionSpoutExecutor**

**Bolt:  ITransactionBoltExecutor,  ITransactionStatefulBoltExecutor**

应用的消息处理接口和JStorm的基础API一致，如果用户从原有的JStorm任务迁移过来，消息的处理逻辑不用改变。只需要在spout和状态bolt节点实现以下状态处理相关的接口。


```java
        /**
          *  Init state from user checkpoint
          *  @param userState user checkpoint state 
          */
        public  void  initState(Object  userState);

        /**
          *  Called when current batch is finished
          *  @return user state to be committed
          */
        public Object finishBatch();

        /**
          *  Commit a batch  
          *  @param The user state data
          *  @return snapshot state which is used to retrieve the persistent user state
          */
        public Object commit(BatchGroupId id, Object state);

        /**
          *  Rollback from last success checkpoint state  
          *  @param user state for rollback
          */
        public void rollBack(Object userState);

        /**
          *  Called when the whole topology finishes committing
          */
        public void ackCommit(BatchGroupId id);
```

*  initState和rollback:  是在任务起来和回滚时，用于让当前节点回到最后一次成功batch的checkpoint。
*  finishBatch:  是用于通知用户，当前batch接收完毕。返回值为当前batch对应的业务checkpoint。
*  commit:  在用户返回对应的checkpoint后，我们会在一个异步线程里调用commit接口，对这个checkpoint进行提交操作，以不阻塞task  execute处理线程。如果checkpoint不大，可以直接在这个接口中返回checkpoint，JStorm最终会在topology  master里统一做持久化。如果checkpoint比较大。用户可以在这个接口中，把对应的checkpoint缓存一份到相应外部存储里，然后返回对应的key。在回滚时，JStorm会把该key值返回给用户，用于找回对应的checkpoint。
*  ackCommit:  当topology任务在所有节点成功后，该接口会被回调。如果之前有checkpoint的缓存，用户在这个接口里可以去完成一些清理工作。比如batch-10完成了，那我们可以删除batch-9和之前的所有checkpoint缓存。

参考例子: sequence-split-merge模块中TransactionTestTopology.java类。
        

##  **架构优势**
相对于Trident和Acker机制。这套架构的最大优势在于:
        1.  每个节点只关心自己节点的提交状态。不用等待所有节点都成功后，再开始下一个batch的提交和计算。
        2.  不需要再依赖acker节点，减少额外系统计算和带宽消耗。原有的acker模型下，每条用户消息都会产生一条对应的系统消息到acker，同时有额外的计算消耗，并且acker消息会消耗大量的网络带宽。
        

##  **相关配置**
```
    # true:  只处理一次;  false:  至少处理一次
    transaction.exactly.once.mode:  true
```


##  **测试结果**  

下图是最多处理一次(关闭所有的消息保证机制)，新的至少处理一次框架和原有的Acker机制的性能对比结果。新的至少处理一次方案性能为原有Acker方案的4~7倍

![img]({{site.baseurl}}/img/programguide/transaction-perf.png)
