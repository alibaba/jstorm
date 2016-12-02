---
title: Stream Split & Merge
layout: plain
top-nav-title: Stream Split & Merge
top-nav-group: advance
top-nav-pos: 5
sub-nav-title: Stream Split & Merge
sub-nav-group: advance
sub-nav-pos: 5
---
> #### **All code can be found in [example code](https://github.com/longdafeng/storm-examples)**

Data stream often needs to be split and merged as the figure shown below. 

![topology]({{site.baseurl}}/img/advance/split_merge/topology.jpg)

## Split

Generally, there are two kinds of split operation.
- Send the same tuple to different kinds bolts
- Different kind of bolt, different tuple

### Send the same tuple to different bolts
Like the one-to-one send-receive mode, there is only one sender(spout or bolt), but  two or more different bolt receiver. For this scenario, no much effort is required. We just need to define one more next level bolt to receive tuple.

Following is the example code:

**Define topology:**

```
SpoutDeclarer spout = builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
        new SequenceSpout(), spoutParal);

builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
                
builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SEQUENCE_SPOUT_NAME);
```

### Different bolt, different tuple

In JStorm, "stream" is used to define the data flow. 

For example, there are one sender and two receivers(Receiver-A and Receiver-B). Receiver-A is defined to receive tuples from Stream-A while Receiver-B is defined to receive tuples from Stream-B. For this scenario, when sender is trying to send tuple through Stream-A, only Receiver-A will receive the tuples, vice versa.

Following is the example code:

**Define topology:**

```
builder.setBolt(SequenceTopologyDef.SPLIT_BOLT_NAME, new SplitRecord(), 2).shuffleGrouping(
        SequenceTopologyDef.SEQUENCE_SPOUT_NAME);

builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME,  // --- Sender Name
        SequenceTopologyDef.TRADE_STREAM_ID); // --- The stream which bolt receives tuples from sender
                
builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME,     // --- Sender Name
        SequenceTopologyDef.CUSTOMER_STREAM_ID); // --- The stream which bolt receives tuples from sender
```

**Send tuples:**

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
            
        collector.emit(SequenceTopologyDef.CUSTOMER_STREAM_ID, 
                 new Values(tupleId, customer));
    }else if (obj != null){
         LOG.info("Unknow type " + obj.getClass().getName());
    }else {
         LOG.info("Nullpointer " );
    }
}
```

**Define the output streams:**

```
public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(SequenceTopologyDef.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
    declarer.declareStream(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
}
```

**When receiving tuples, the checking of source source stream is required.**

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

## Merge

JStorm is able to make one bolt receive tuples from more than two stream types to do the merge operation by defining more than two source stream type.

Following is an example. MergeRecord bolt receives tuples from SequenceTopologyDef.TRADE_BOLT_NAME and SequenceTopologyDef.CUSTOMER_BOLT_NAME at the same time.

**Define topology:**

```
builder.setBolt(SequenceTopologyDef.TRADE_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME, 
        SequenceTopologyDef.TRADE_STREAM_ID);
                
builder.setBolt(SequenceTopologyDef.CUSTOMER_BOLT_NAME, new PairCount(), 1).shuffleGrouping(
        SequenceTopologyDef.SPLIT_BOLT_NAME,
        SequenceTopologyDef.CUSTOMER_STREAM_ID);
                
builder.setBolt(SequenceTopologyDef.MERGE_BOLT_NAME, new MergeRecord(), 1)
        .shuffleGrouping(SequenceTopologyDef.TRADE_BOLT_NAME)      // --- Define two source stream
        .shuffleGrouping(SequenceTopologyDef.CUSTOMER_BOLT_NAME);
```

**Sender:**
No changes are required for the sender.

**Receiver:**
For receiver, getSourceComponent() api can be used to distinguish which source component the tuple is sent from.
 
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