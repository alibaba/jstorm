---
title:  "How to use Trident?"
layout: plain_cn

# Top-level navigation
top-nav-group: ProgrammingGuide_cn
top-nav-pos: 4
top-nav-id: Trident_cn
top-nav-title: Trident


sub-nav-group: Trident_cn
sub-nav-id: Trident_cn
sub-nav-pos: 1
sub-nav-title: 概述


---

* This will be replaced by the TOC
{:toc}


# 概述

Trident是Storm 0.8.0版本引入的新特性，为基于Storm元语进行实时计算的用户提供了一类更高级的抽象元语，能够同时满足高吞吐量(每秒百万级的消息)与低处理延时。Trident提供了Partition-local,Repartitioning, Aggregation, Group, Merges and joins这五类基本元语，同时，Trident延续了TransactionalTopology的exactly-once的语义，能够满足用户一些事务性的需求，但前提条件是对性能要有所牺牲。


# 例子
举一个[官方文档](https://github.com/alibaba/jstorm/blob/master/example/sequence-split-merge/src/main/java/com/alipay/dw/jstorm/example/trident/TridentWordCount.java)中的例子，来简单介绍一下Trident的功能

这个例子中要完成两个功能:

统计word count

通过执行查询得到一组单词的总数

 
下面代码为模拟输入的Spout代码

```
FixedBatchSpoutspout=newFixedBatchSpout(

newFields("sentence"),3,

    newValues("the cow jumped over the moon"),

newValues("the man went to the store

   and bought some candy"),

    newValues("four score and seven years ago"),

    newValues("how many apples can you eat"),

spout.setCycle(true);                  

//true表示会循环的产生以上values

 

如果我们要用Storm的基本元语来实现这个功能应该怎么做呢？

// 创建一个TopologyBuilder

TopologyBuilderbuilder = newTopologyBuilder();

// 设置spout,component id设为sentences

builder.setSpout("sentences",spout,5);

// 设置分句的bolt, 上游component为sentences

builder.setBolt("split", new SplitSentence(), 8)

    .shuffleGrouping("sentences");

// 设置统计word count的bolt, 上游component

为split, grouping策略是按word做hash

builder.setBolt("count", new WordCount(), 12)

    .fieldsGrouping("split",new Fields("word"));

 

// 再创建一个DRPC client, 但外部查询调用

DRPCClientclient = newDRPCClient(

"drpc.server.location",port);

/*

 …//定义DRPC的代码

*/
```

其中，SplitSentence与WordCount是两个Bolt类。

 

而对于Trident来说，如何完成这个功能呢？

一种实现是，只通过Trident进行计算过程的描述,再单独创建一个DRPC client供查询调用

```
TridentTopologytopology = newTridentTopology();

TridentStatewordCounts =

     topology.newStream("spout1",spout)

       .each(

           new Fields("sentence"),

           new Split(),

           new Fields("word"))

       .groupBy(new Fields("word"))

       .persistentAggregate(

           new MemoryMapState.Factory(),

           new Count(),

           new Fields("count"))               

       .parallelismHint(6);

// 再创建一个DRPC client, 但外部查询调用

DRPCClientclient = newDRPCClient(

       "drpc.server.location",port);

/*

…//定义DRPC的代码

*/

 

另一种实现是直接通过Trident的Topology创建一个DRPCStream

topology.newDRPCStream("words")

       .each(new Fields("args"),

          new Split(),

          new Fields("word"))

       .groupBy(new Fields("word"))

       .stateQuery(wordCounts,

          new Fields("word"),

          new MapGet(),

          new Fields("count"))

       .each(new Fields("count"),

          new FilterNull())

       .aggregate(new Fields("count"),

          new Sum(),

          new Fields("sum"));

``` 

而在上面的例子中，Split和Count, Sum是简单的Function，而不再是Bolt。

可以看出，通过Trident这层更高级的抽象层，用户再也看不到bolt，ack, emit, batchBolt这样的概念，用户也不用纠结于execute和finishBatch里到底该写些啥。只需要根据业务需求的数据处理逻辑，对Stream进行简单的groupBy, aggregate等操作。而这些数据操作的基本概念显然是很容易理解的。

当然以上只是Trident从API层面带来的最直观的优势，Trident更强大的地方在下文中会逐一介绍。

 

#  基本概念
对于Trident来说，用户所要关心的对象只有Stream, Trident的所有API也都是针对Stream。

所以基本元语也不再是Storm的Spout, Bolt, Acker等，而是五类新的元语：

 

## Partition-local operations

不需要网络传输，在local就能完成的操作

包括以下API:

a.       Function:用户自定义的Tuple操作函数.:

b.       Filters:对Tuple进行过滤，如果isKeep返回false则过滤掉该Tuple

c.      partitionAggregate: 对同一个batch的数据进行local combiner操作

d.      project: 只保留stream中指定的Field

e.      stateQuery和partitionPersist: partitionPersist会被计算的中间状态(state)写到持久化存储中，stateQuery可以从持久化查询中获得当前的state信息

 

## Repartition operations

 决定Tuple如何分发到下一个处理环节的操作

包括以下API:

a.      shuffle: 采用random round robin算法将Tuple均匀的分发到所以目标分区

b.      broatcase: 以广播的形式发分Tuple, 所有的分区都会收到

c.      partitionBy: 以某一个特定的Field进行Hash求余，分到某一个partition, 该Field值相同的Tuple将会分到同一个partition,会被顺序执行

d.      global: 所有tuple将被发到指定的相同的分区

e.      batchGlobal: 同一批的tuple将被发到相同的分区，不同的批次发到不同的分区

f.       partition: 用户自定义的分区策略

 

## Aggregation operations

不同partition处理结果的汇聚操作

包括以下API:

a.      aggregation: 只针对同一批的数据进行汇聚操作

b.      persistentAggregation: 针对所以批次的数据进行汇聚操作，并将计算的中间状态(state)保存在持久化存储中

 

## groupBy operations

 对stream中的tuple进行重新分组，后续的操作将针对每一个分组独立进行，类似于sql里的groupBy

a.      groupBy

 

## Merges and Joins operations

 将多个Stream融合为一个Stream

包括以下API：

a.      merge: 多个流进行简单的合并

b.      join: 多个流按照某一个key进行union操作，join只能针对同一个batch的数据。

 

# 一致性

Trident对进行读写资源的操作做了非常好的抽象工作，将这些有状态的操作全部融入到TridentTopology的内部，用户可以帮Trident运行过程中间状态（State）写到Hbase, Mysql等相关Nosql, DB产品中，这些操作对于Trident的API来说，是没有任何区别的。

Trident通过以一种幂等的形式对State进行管理，做到即使在发生重试或操作处理节点挂掉的情况，也能保证每一个Tuple会被处理一次(exactly-once semantics)。
        

Trident定义了三个基本的语义，就可以实现exactly once的功能：

a.      所有的Tuples是以小批量的形式进行处理，而不是Storm基本功能中的单条数据处理。

b.      在每一批数据系统处理的过程中，会生成一个严格递增的事务id(txid), 如果这批数据因为处理超时或者没有正确处理完，则这批数据将会被重发，重发后txid保持不变。

c.      不同批次数据的计算是乱序执行，但中间状态(State)的提交是按照txid的顺序进行提交的。上一批数据的计算结果没有被正确提交，下一批数据的结果是不会被提交的。

 
 

当然，要做到exactly-once还需要源头spout的配合。根据对不同容错级别的定义，Trident将spout分为三类，non-transactionalspout, transactional spout, opaque transactional spout。

 

Non-transactionalspout

顾名思义，spout对每一批数据的容错处理不做任何保证，使用这类spout无法做到exactly once。如果一批数据处理失败进行不重发的话，那么只能做到数据至多被处理一次；如果一批数据处理失败重发的话，那么只能做到数据至少被处理一次，但是有可能出现同一条数据被处理多次。

 

Transactionalspout

事务性的spout, 使用这类spout，用户可以很轻易做到exactly-once, 但是这类spout对消息队列要三个基本要求：

a.      Trident会给每批数据分配一个txid, 给定一个txid，得到的批次是相同, 消息队列重发过来的数据必须与第一次来的数据有相同的txid。

b.      不同批的数据之间不能有重叠部分

c.      每一个tuple必须置于一个批次中，不能漏数据。

 

使用Transactional spout时，用户维护中间状态的代码会变的非常简单，并且可以做到只处理一次，非常适用于counter场景。

举一个WordCount的例子。

数据库中当时状态：

man=> [count=3, txid=1]

dog=> [count=4, txid=3]

apple=> [count=10, txid=2]

现在来了一批数据，txid=3

["man"]

["man"]

["dog"]

对于”man”来说，最后一次来的txid=1，所以txid=3的数据是需要处理的。而对于”dog”来说，最后一次来的txid=3，所以txid=3的数据已经处理过，所以这批是重发过来的，不做处理。最后得到的结果是：

man=> [count=5, txid=3]

dog=> [count=4, txid=3]

apple=> [count=10, txid=2]

 

如果你所使用消息队列无法保证在系统可以根据指定的txid取得同一批的数据，那么，你就没法满足transactional spout第一个基本要求，transactionalspout就无能为力了。

 
Opaque transactional spout

相比Transactional spout而言，Opaque transactional spout对消息队列的要求要宽松一些：

a.      每一个tuple都会在一个批次的数据中，如果在tuple在某一批处理失败，可以将这个tuple重发到后续的批次中。

b.      不同批数据不能有重叠

c.      不可以漏数据


Transactional spout是透明的，用户只需要记录中间结果和最后一次成功处理的txid，就能完成exactly-once的语义。而对于Opaque transactional spout来说，对消息队列的要求宽松一些，但是对于用户的操作来说，却无法做到透明。用户需要记录一些历史结果，配合Trident完成exactly-once的语义。

再拿word count举例，假设一个word的当时状态如下：

```
{

value = 23,     //当时统计的值

  prevValue =1,  // 在txid这批来之前的值

  txid =2         // 最后一次处理过的批txid

}
```
 

假设现在来了一批数据, 这一批中，这个word的统计值是20，txid=3

因为最新txid 是3，大于最后一次处理过的txid, 所以，这次是新来的数据，最终结果值为原来的value加上这批新来的值，prevValue设为原来的value，最后一次处理过的txid设为3,最终的结果为

```
{

value = 43

  prevValue =23,

  txid =3

}
```
 

但如果现在来的这批数据，txid=2，我们又将进行怎样的逻辑处理呢？

因为，最后一次处理过的txid也等于2，说明在处理txid=2的这批数据时，整个stream没有被完整的处理完，所以这批数据是重发的。那么就意味着要重新处理，所以，新的value应该为prevValue+20，最终的结果为

```
{

  value = 21,

  prevValue =1,

  txid =2

}
```

这时有个疑问，在第一次计算txid=2的这批数据时，得到的结果是23，而新的这批数据计算结果却为21，少了的那两条数据是丢了吗？

这两条数据并没有丢失

每一个tuple都会在一个批次的数据中，如果在tuple在某一批处理失败，可以将这个tuple重发到后续的批次中。

这两条数据将在后续的批次中重发，如果在还是像处理Transactionalspout那样，txid=2的这批数据，直接不处理，就会导致多计算。

 

通过上述对三类spout的比较，可以得出如下结论：

a.      Non-transactional spout无法做到exactly-once

b.      Transactional spout可以做到exactly-once，且对于用户来说，使用上的透明的。不需要关注值的中间状态，但对数据源头的要求是比较苛刻的

c.      Opaque transactional spout也可以做到达exactly-once，且功能是最强的。但对于用户的使用上来说，需要精心的设计中间状态（State）的存储。对数据源头的要求相对要低一些。




