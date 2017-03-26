---
title:  "How to split one stream or join multiple stream"
# Top-level navigation
top-nav-group: ProgrammingGuide_cn
top-nav-pos: 2
top-nav-id: StreamSplitJoin_cn
top-nav-title: 数据流分流合并
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


# 概述

数据流经常需要分流与合并操作，如下图所示：


![topology]({{site.baseurl}}/img/advance/split_merge/topology.jpg)

请参考[示例代码](https://github.com/alibaba/jstorm/tree/master/example/sequence-split-merge)


# 分流

分流有2钟情况，第一种是，相同的tuple发往下一级不同的bolt， 第二种，分别发送不同的tuple到不同的下级bolt上。

## 发送相同tuple

其实和普通1v1 发送一模一样，就是有2个或多个bolt接收同一个spout或bolt的数据
举例来说：

```
SpoutDeclarer spout = builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
                new SequenceSpout(), spoutParal);

builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
                        SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
                
builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1)
                        .shuffleGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
```

## 发送不同的tuple

当发送不同的tuple到不同的下级bolt时， 这个时候，就需要引入stream概念，发送方发送a 消息到接收方A'时使用stream A， 发送b 消息到接收方B'时，使用stream B

### 在topology提交时：

```
    builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SplitRecord(), 2).shuffleGrouping(
        SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
                
    builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME,  // --- 发送方名字
        SequenceTopologyDef.TRADE_STREAM_ID); // --- 接收发送方该stream 的tuple
                
    builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1)
        .shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME, // --- 发送方名字
        SequenceTopologyDef.CUSTOMER_STREAM_ID);      // --- 接收发送方该stream 的tuple
```

### 在发送消息时：

在发送消息时， 需要注明消息属于那个流， 

```
public void execute(Tuple tuple, BasicOutputCollector collector) {
     tpsCounter.count();
     
     Long tupleId = tuple.getLong(0);
     Object obj = tuple.getValue(1);
     
     if (obj instanceof TradeCustomer) {
     
         TradeCustomer tradeCustomer = (TradeCustomer)obj;
         
         Pair trade = tradeCustomer.getTrade();
         Pair customer = tradeCustomer.getCustomer();
            
            collector.emit(SequenceTopologyDef.TRADE_STREAM_ID, 
                    new Values(tupleId, trade));
            //SequenceTopologyDef.TRADE_STREAM_ID就是流名称
            
            collector.emit(SequenceTopologyDef.CUSTOMER_STREAM_ID, 
                    new Values(tupleId, customer));
            // SequenceTopologyDef.CUSTOMER_STREAM_ID 就是流名称
     }else if (obj != null){
         LOG.info("Unknow type " + obj.getClass().getName());
     }else {
         LOG.info("Nullpointer " );
     }
  
 }
```

### 定义输出流格式：

```
public void declareOutputFields(OutputFieldsDeclarer declarer) {
  declarer.declareStream(SequenceTopologyDef.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
  declarer.declareStream(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
 }
```

接受消息时，需要判断数据流

```
 if (input.getSourceStreamId().equals(SequenceTopologyDef.TRADE_STREAM_ID) ) {
            customer = pair;
            customerTuple = input;
            
            tradeTuple = tradeMap.get(tupleId);
            if (tradeTuple == null) {
                customerMap.put(tupleId, input);
                return;
            }
            
            trade = (Pair) tradeTuple.getValue(1);
            
        }
```

# 数据流合并

## 生成topology时

在下面例子中， MergeRecord 同时接收SequenceTopologyDef.TRADE_BOLT_NAME 和SequenceTopologyDef.CUSTOMER_BOLT_NAME 的数据

```
    builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME, 
        SequenceTopologyDef.TRADE_STREAM_ID);
                
    builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1)
        .shuffleGrouping(SequenceTopologyDef.SPLIT_BOLT_NAME,
        SequenceTopologyDef.CUSTOMER_STREAM_ID);
                
    builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new MergeRecord(), 1)
        .shuffleGrouping(SequenceTopologyDef.TRADE_BOLT_NAME)
        .shuffleGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME);
```

## 发送方

发送的bolt和普通一样，无需特殊处理

## 接收方
接收方是，区分一下来源component即可识别出数据的来源

```
        if (input.getSourceComponent().equals(SequenceTopologyDef.CUSTOMER_BOLT_NAME) ) {
            customer = pair;
            customerTuple = input;
            
            tradeTuple = tradeMap.get(tupleId);
            if (tradeTuple == null) {
                customerMap.put(tupleId, input);
                return;
            }
            
            trade = (Pair) tradeTuple.getValue(1);
            
        } else if (input.getSourceComponent().equals(SequenceTopologyDef.TRADE_BOLT_NAME)) {
            trade = pair;
            tradeTuple = input;
            
            customerTuple = customerMap.get(tupleId);
            if (customerTuple == null) {
                tradeMap.put(tupleId, input);
                return;
            }
            
            customer = (Pair) customerTuple.getValue(1);
        } 
```

