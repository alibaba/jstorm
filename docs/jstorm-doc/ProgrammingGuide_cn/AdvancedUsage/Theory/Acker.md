---
title: "Acker"
layout: plain_cn

# Sub navigation
sub-nav-parent: Theory_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Acker_cn
#sub-nav-pos: 1
sub-nav-title: Acker 原理
---

* This will be replaced by the TOC
{:toc}

# JStorm Acker详解

---

### acker概述

JStorm的acker机制，能够保证消息至少被处理一次（at least once）。也就是说，能够保证不丢消息。这里就详细解析一下acker的实现原理。

### 消息流

假设我们有一个简单的topology，结构为spout -> bolt。
spout emit了一条消息，发送至bolt。bolt作为最后一个处理者，没有再向下游emit消息。

<center>
  <img src="{{site.baseurl}}/img/programguide/AdvancedUsage/acker_1.png" width="900px">
</center>

从上图可以看到，所有的ack消息都会发送到acker，acker会根据算法计算从特定spout发射出来的tuple tree是否被完全处理。如果成功处理，则发送\__acker_ack消息给spout，否则发送\__acker_fail消息给spout。然后spout中可以做相应的逻辑如重发消息等。


在JStorm中，acker是一种bolt，因此它的处理、消息发送跟正常的bolt是一样的。只不过，acker是JStorm框架创建的bolt，用户不能自行创建。如果用户在代码中使用：

```
Config.setNumAckers(conf, 1);
```

就会自动创建并行度为1的acker bolt；如果为0，则就没有acker bolt了。

### 如何判断消息是否被成功处理？
acker的算法非常巧妙，它利用了数学上的异或操作来实现对整个tuple tree的判断。在一个topology中的一条消息形成的tuple tree中，所有的消息，都会有一个MessageId，它内部其实就是一个map：

```
Map<Long, Long> _anchorsToIds;
```

存储的是anchor和anchor value。而anchor其实就是root_id，它在spout中生成，并且一路透传到所有的bolt中，属于同一个tuple tree中的消息都会有相同的root_id，它可以唯一标识spout发出来的这条消息（以及从下游bolt根据这个tuple衍生发出的消息）。

下面是一个tuple的ack流程：

1. spout发送消息时，先生成root_id。
2. 对每一个目标bolt task，生成`<root_id, random()>`，即为这个root_id对应一个随机数值，然后随着消息本身发送到下游bolt中。假设有2个bolt，生成的随机数对分别为：`<root_id, r1>`, `<root_id, r2>`。
3. spout向acker发送ack_init消息，它的MessageId = `<root_id, r1 ^ r2>`（即所有task产生的随机数列表的异或值）。
4. bolt收到spout或上游bolt发送过来的tuple之后，首先它会向acker发送ack消息，MessageId即为收到的值。同时，如果bolt下游还有bolt，则跟步骤2类似，会对每一个bolt，生成随机数对，root_id相同，但是值变为`当前值 ^ 新生成的随机数`。以此类推。
5. acker收到消息后，会对root_id下所有的值做异或操作，如果算出来的值为0，表示整个tuple tree被成功处理；否则就会一直等待，直到超时，则tuple tree处理失败。
6. acker通知spout消息处理成功或失败。

---

我们以一个稍微复杂一点的topology为例，描述一下它的整个过程。
假设我们的topology结构为：
`spout -> bolt1/bolt2 -> bolt3`
即spout同时向bolt1和bolt2发送消息，它们处理完后，都向bolt3发送消息。bolt3没有后续处理节点。

<center>
  <img src="{{site.baseurl}}/img/programguide/AdvancedUsage/acker_2.png" width="900px">
</center>

1). spout发射一条消息，生成root_id，由于这个值不变，我们就用root_id来标识。
spout -> bolt1的MessageId = `<root_id, 1>`
spout -> bolt2的MessageId = `<root_id, 2>`
spout -> acker的MessageId = `<root_id, 1^2>`

2). bolt1收到消息后，生成如下消息：
bolt1 -> bolt3的MessageId = `<root_id, 3>`
bolt1 -> acker的MessageId = `<root_id, 1^3>`

3). 同样，bolt2收到消息后，生成如下消息：
bolt2 -> bolt3的MessageId = `<root_id, 4>`
bolt2 -> acker的MessageId = `<root_id, 2^4>`

4). bolt3收到消息后，生成如下消息：
bolt3 -> acker的MessageId = `<root_id, 3>`
bolt3 -> acker的MessageId = `<root_id, 4>`

5). acker中总共收到以下消息：
`<root_id, 1^2>`
`<root_id, 1^3>`
`<root_id, 2^4>`
`<root_id, 3>`
`<root_id, 4>`
所有的值进行异或之后，即为`1^2^1^3^2^4^3^4` = 0。

---

### 代码分析
实现ack的代码，主要在这几个类中：`SpoutCollector`, `BoltCollector`, `Acker`。

其中`SpoutCollector.sendSpoutMsg方法`：

```java
    private List<Integer> sendSpoutMsg(String out_stream_id, List<Object> values, Object message_id, Integer out_task_id) {
        final long startTime = System.nanoTime();
        try {
            // 得到目标task id列表
            java.util.List<Integer> out_tasks;
            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values);
            }

            if (out_tasks.size() == 0) {
                // don't need send tuple to other task
                return out_tasks;
            }
            List<Long> ackSeq = new ArrayList<Long>();
            Boolean needAck = (message_id != null) && (ackerNum > 0);

            // 生成随机的root_id，但是需要确保在当前spout中不能有重复的，不然就不能保证ack的准确性了
            Long root_id = MessageId.generateId(random);
            if (needAck) {
                while (pending.containsKey(root_id)) {
                    root_id = MessageId.generateId(random);
                }
            }
            
            // 遍历所有的目标task，每个task的messageId=<root_id, 随机数值>
            for (Integer t : out_tasks) {
                MessageId msgid;
                if (needAck) {
                    Long as = MessageId.generateId(random);
                    msgid = MessageId.makeRootId(root_id, as);
                    // 添加到ackSeq list中，后面会有用
                    ackSeq.add(as);
                } else {
                    msgid = MessageId.makeUnanchored();
                }

                // 扔到transfer queue中，即进入发送队列
                TupleImplExt tp = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                transfer_fn.transfer(tp);
            }

            // ack消息的逻辑在这里面，上面对所有的目标task分别emit消息，但是ack_init消息只需要发送一条。
            if (needAck) {
                TupleInfo info = new TupleInfo();
                info.setStream(out_stream_id);
                info.setValues(values);
                info.setMessageId(message_id);
                info.setTimestamp(System.nanoTime());

                pending.putHead(root_id, info);

                // messageId = <root_id, 所有目标task的messageId随机数值的异或>
                List<Object> ackerTuple = JStormUtils.mk_list((Object) root_id, JStormUtils.bit_xor_vals(ackSeq), task_id);

                // 发送给acker。会根据__acker_init这个stream直接找到task id进行发送。
                UnanchoredSend.send(topology_context, sendTargets, transfer_fn, Acker.ACKER_INIT_STREAM_ID, ackerTuple);

            } else if (message_id != null) {
                // 这里的逻辑，处理没有acker，但是仍然实现了IAckValueSpout接口的情况，需要给这种spout回调ack方法的机会。
                TupleInfo info = new TupleInfo();
                info.setStream(out_stream_id);
                info.setValues(values);
                info.setMessageId(message_id);
                info.setTimestamp(0);

                AckSpoutMsg ack = new AckSpoutMsg(spout, null, info, task_stats, isDebug);
                ack.run();
            }

            return out_tasks;
        } finally {
            long endTime = System.nanoTime();
            emitTotalTimer.update((endTime - startTime) / TimeUtils.NS_PER_US);
        }
    }
```

再来看一下`BoltCollector`类的逻辑，通常来说bolt是先execute（先emit），再执行ack方法。因此先看boltEmit方法：

```java
    private List<Integer> boltEmit(String out_stream_id, Collection<Tuple> anchors, List<Object> values, Integer out_task_id) {
        final long start = System.nanoTime();
        try {
            // 一样地获取所有目标task列表
            java.util.List<Integer> out_tasks;
            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values);
            }

            // 遍历所有目标task，每一个目标task的message id= <root_id, edge_id>，其中edge_id是在这个bolt里新生成的随机数
            for (Integer t : out_tasks) {
                Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
                if (anchors != null) {
                    // 在一般的情况下anchors的size=1，见BasicOutputCollector类，即为当前收到的inputTuple。
                    for (Tuple a : anchors) {
                        Long edge_id = MessageId.generateId(random);
                        long now = System.currentTimeMillis();
                        // 这里是提前删除可能的超时tuple
                        if (now - lastRotate > rotateTime) {
                            pending_acks.rotate();
                            lastRotate = now;
                        }
                        // 这里会将<inputTuple, edge_id>放入pending_acks
                        put_xor(pending_acks, a, edge_id);
                        // 这里将每一对<root_id, edge_id>放入anchors_to_ids（一般情况下也只有一对），由于anchors_to_ids是一个空map，因此put_xor里面，相当于拿root_id对应的值^0 = root_id的值
                        for (Long root_id : a.getMessageId().getAnchorsToIds().keySet()) {
                            put_xor(anchors_to_ids, root_id, edge_id);
                        }
                    }
                }
                
                // 往目标bolt发送消息
                MessageId msgid = MessageId.makeId(anchors_to_ids);
                TupleImplExt tupleExt = new TupleImplExt(topologyContext, values, task_id, out_stream_id, msgid);
                tupleExt.setTargetTaskId(t);
                taskTransfer.transfer(tupleExt);
            }
            return out_tasks;
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            long end = System.nanoTime();
            timer.update((end - start) / TimeUtils.NS_PER_US);
        }
        return new ArrayList<Integer>();
    }
```

emit完之后，再来看ack的逻辑：

```
    public void ack(Tuple input) {
        if (ackerNum > 0) {
            Long ack_val = 0L;
            // 这里取出boltEmit放入的对象：<inputTuple, edge_id>
            Object pend_val = pending_acks.remove(input);
            if (pend_val != null) {
                // ack_val = edge_id
                ack_val = (Long) (pend_val);
            }

            // 发送ack消息，messageId = <root_id, inputTuple的随机数 ^ edge_id>
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                UnanchoredSend.send(topologyContext, sendTargets, taskTransfer, Acker.ACKER_ACK_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey(), JStormUtils.bit_xor(e.getValue(), ack_val)));
            }
        }

        Long startTime = (Long) tuple_start_times.remove(input);
        if (startTime != null) {
        	Long endTime = System.nanoTime();
        	long latency = (endTime - startTime)/TimeUtils.NS_PER_US;
        	long lifeCycle = (System.currentTimeMillis() - ((TupleExt) input).getCreationTimeStamp()) * TimeUtils.NS_PER_US;
        	
            task_stats.bolt_acked_tuple(input.getSourceComponent(), input.getSourceStreamId(), latency, lifeCycle);
        }
    }
```

最后就是acker了，这个逻辑比较简单：

```java
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        AckObject curr = pending.get(id);
        String stream_id = input.getSourceStreamId();
        // __acker_init消息，由spout发送，直接放入pending map中
        if (Acker.ACKER_INIT_STREAM_ID.equals(stream_id)) {
            if (curr == null) {
                curr = new AckObject();

                curr.val = input.getLong(1);
                curr.spout_task = input.getInteger(2);

                pending.put(id, curr);
            } else {
                // bolt's ack first come
                curr.update_ack(input.getValue(1));
                curr.spout_task = input.getInteger(2);
            }

        } else if (Acker.ACKER_ACK_STREAM_ID.equals(stream_id)) {
            // __ack_ack消息
            if (curr != null) {
                curr.update_ack(input.getValue(1));
            } else {
                // two case
                // one is timeout
                // the other is bolt's ack first come
                curr = new AckObject();
                curr.val = input.getLong(1);
                pending.put(id, curr);
            }
        } else if (Acker.ACKER_FAIL_STREAM_ID.equals(stream_id)) {
            // 也有可能直接fail了
            if (curr == null) {
                // do nothing
                // already timeout, should go fail
                return;
            }
            curr.failed = true;
        } else {
            LOG.info("Unknow source stream");
            return;
        }

        // 告诉spout这个消息ack/fail了
        Integer task = curr.spout_task;
        if (task != null) {
            if (curr.val == 0) {
                pending.remove(id);
                List values = JStormUtils.mk_list(id);
                collector.emitDirect(task, Acker.ACKER_ACK_STREAM_ID, values);
            } else {
                if (curr.failed) {
                    pending.remove(id);
                    List values = JStormUtils.mk_list(id);
                    collector.emitDirect(task, Acker.ACKER_FAIL_STREAM_ID, values);
                }
            }
        } else {

        }

        // 这里只是更新metrics
        // add this operation to update acker's ACK statics
        collector.ack(input);

        long now = System.currentTimeMillis();
        if (now - lastRotate > rotateTime) {
            lastRotate = now;
            Map<Object, AckObject> tmp = pending.rotate();
            LOG.info("Acker's timeout item size:{}", tmp.size());
        }
    }
```

### 如何使用acker

1. 设置acker的并发度要>0;
2. spout发送消息时，使用的接口List<Integer> emit(List<Object> tuple, Object messageId)；其中messageId由用户指定生成，用户消息处理成功或者失败后，用于对public void ack(Object messageId) 和public void fail(Object messageId) 接口的回调；
3. 如果spout同时从IAckValueSpout和IFailValueSpout派生，那么要求实现void fail(Object messageId, List<Object> values)和void ack(Object messageId, List<Object> values);这两接口除了会返回messageId，还会返回每一条消息；
4. bolt一般从如果从IRichBolt派生，发送消息到下游时要注意以下两种不同类型的接口：
```
	public List<Integer> emit(Tuple anchor, List<Object> tuple); //anchor 代表当前bolt接收到的消息， tuple代表发送到下游的消息
    public List<Integer> emit(List<Object> tuple);
    //如果对即将发送的消息不打算acker的话，可以直接用第二种接口；如果需要对即将发送的下游的消息要进行acker的话，emit的时候需要携带anchor
```

5. 如果bolt接收到的消息是需要被acker的话，记得在execute里头别忘了执行_collector.ack(tuple)操作；例子如下
```
    @Override
    public void execute(Tuple tuple) {
        _collector.emit(tuple, new Values(tuple.getString(0)));
        _collector.ack(tuple);
    }
```	
	
6. 对于从IRichBolt派生的的bolt来说是不是很麻烦，即要求采样合适的emit接口，还要求主动执行acker操作，那么好消息来了如果当前bolt是从IBasicBolt派生的话，内部都会帮你执行这些操作，你只管调用emit(List<Object> tuple)发送消息即可；

7. 例子如下

```
public class PairCount implements IBasicBolt {
    private static final long serialVersionUID = 7346295981904929419L;

    public static final Logger LOG = LoggerFactory.getLogger(PairCount.class);

	private AtomicLong  sum = new AtomicLong(0);
	
	private TpsCounter          tpsCounter;

	public void prepare(Map conf, TopologyContext context) {
	    tpsCounter = new TpsCounter(context.getThisComponentId() + 
                ":" + context.getThisTaskId());
		
		LOG.info("Successfully do parepare " + context.getThisComponentId());
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    tpsCounter.count();
	    
		Long tupleId = tuple.getLong(0);
		Pair pair = (Pair)tuple.getValue(1);
        
        sum.addAndGet(pair.getValue());
        
        // 如果需要ack，只需要这么做：
        collector.emit(new Values(tupleId, pair)); 
	}

	public void cleanup() {
	    tpsCounter.cleanup();
		LOG.info("Total receive value :" + sum);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ID", "PAIR"));
	}

	public Map<String, Object> getComponentConfiguration() {
	    // TODO Auto-generated method stub
	    return null;
	}
}
```
