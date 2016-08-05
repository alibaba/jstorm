---
title: "IBasicBolt 接口介绍"
layout: plain_cn

# Sub navigation
sub-nav-parent: API_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: IBasicBolt_cn
#sub-nav-pos: 3
sub-nav-title: IBasicBolt 介绍
---

* This will be replaced by the TOC
{:toc}

事实上，很多使用JStorm/Storm的人无法分清BasicBolt和IRichBolt之间的区别。我们的建议是尽可能的使用IBasicBolt。

***

IRichBolt继承自IBolt，IBolt会使用OutputCollector来发送元组。

```java
public interface IBolt extends Serializable {
...
  void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
...
}
```
OutputCollector有两个用于发送元组的函数：

```java
//后续component会向acker发送ack响应。
List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple)
//后续component不会向acker发送ack响应。
List<Integer> emit(String streamId, List<Object> tuple) {
```

IBasicBolt使用BasicOutputCollector来发送元组
```java
public interface IBasicBolt extends IComponent {
...
void execute(Tuple input, BasicOutputCollector collector);
...
}
```
BasicOutputCollector只有第二个emit函数。但是这个函数包裹了OutputCollector第一个emit函数来完成工作。

```java 
//out是一个OutputCollector实例.
List<Integer> emit(String streamId, List<Object> tuple) {
        return out.emit(streamId, inputTuple, tuple);
    }
```

因此，在IBasicBolt中，`emit(String streamId, List<Object> tuple)`是用于处理元组的可靠方法。但是，在IRichBolt中，它不是一个可靠的方法。

在使用IRichBolt是，如果你想可靠的处理元组，你应该显式地调用`emit(String streamId, Tuple anchor, List<Object> tuple)` 。