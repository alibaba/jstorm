---
title: IBasicBolt vs IRichBolt
layout: plain
top-nav-title: IBasicBolt vs IRichBolt
top-nav-group: api
top-nav-pos: 2
---
In fact, a lot of people using JStorm/Storm don't know the difference between IBasicBolt and IRichBolt. We suggest using IBasicBolt as much as possible.

***

IRichBolt extends from IBolt that uses OutputCollector to emit tuple.

```java
public interface IBolt extends Serializable {
...
  void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
...
}
```
OutputCollector has two kinds of methods to emit tuple:

```java
//the next component's tasks will send Ack response to Acker.
List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple)
//the next component's tasks won't send Ack response to Acker.
List<Integer> emit(String streamId, List<Object> tuple) {
```

IBasicBolt uses BasicOutputCollector to emit tuple.
```java
public interface IBasicBolt extends IComponent {
...
void execute(Tuple input, BasicOutputCollector collector);
...
}
```

BasicOutputCollector only has the second emit method, but this method wraps the OutputCollector's first one to do the job.

```java 
//out is a OutputCollector instance.
List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, inputTuple, tuple);
    }
```

So in IBasicBolt, `emit(String streamId, List<Object> tuple)` is a reliable method to process tuple, but in IRichBolt, it is not.

If you want to process tuple reliability when you use IRichBolt, you should call `emit(String streamId, Tuple anchor, List<Object> tuple)` explicitly.