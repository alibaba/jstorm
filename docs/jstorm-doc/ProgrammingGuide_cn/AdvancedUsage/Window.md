---
title:  "How to use Window Framework?"
layout: plain_cn

#sub-nav-parent: AdvancedUsage
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Window_cn
sub-nav-pos: 9
sub-nav-title: Window Framework
---

* This will be replaced by the TOC
{:toc}

## 概要
在流式计算中我们经常需要以时间或者数据量将无界的数据划分成一份份有限的集合，然后以这个集合为维度进行操作。比如我们会计算过去1个小时交易额TOP 10的天猫卖家，这时我会按照交易事件发生的时间将交易事件划分成到某个事件集合当中去，每个集合的大小是1个小时，然后计算每一个集合的TOP10。在流式计算中，这类的集合通常我们称之为Window。有了Window概念后，我们可以很方便的做aggregations, joins, pattern matching……

Apache Storm/Jstorm 目前定义了自己的一套Window逻辑，并对Window定义及操作进行了上层抽象。我们完全可以根据以下两个参数定义一个Window:

```
Window length： 可以用时间或者数量来定义窗口大小
Sliding interval： 窗口滑动的间隔
Storm/Jstorm： 支持滚动窗口和滑动窗口，这些窗口都可以基于time或者event count去定义。
```

### 滚动窗口（Tumbling Window）
每个Tuple只能属于其中一个滚动窗口，一个Tuple不能同时是两个或者两个以上窗口的元素。比如下面我们以消息到达的时间来划分成一个个滚动窗口，窗口大小是5秒:


第一个窗口w1包含的是0~5th到达的数据，第二窗口w2包含的是5~10th秒到达的数据，第三个窗口包含的是10~15th秒到达的数据。每个窗口每隔5秒被计算，窗口和窗口直接没有重叠。

### 滑动窗口（Sliding Window）
tuples被划分到一个个滑动窗口，窗口滑动的间隔是sliding interval。每个窗口间会有重叠，重叠部分的tuple可以先后属于前后两个窗口。比如下面我们以事件处理时间划分滑动窗口，窗口大小是10秒，滑动间隔是5秒：


第一个窗口w1包含的是0~10th到达的数据，第二窗口w2包含的是5~15th秒到达的数据。消息时间e3~e6都是窗口w1和w2的一部分。在15th秒时，窗口w2会被计算，这时候e1 e2的数据会会过期，会从队列中移除。

## Window 配置
Storm支持以时间、数量或者以两者混合的方式来定义窗口带下及窗口滑动间隔。

Storm抽象了Window Bolt的接口 IWindowedBolt ：

```
public interface IWindowedBolt extends IComponent {
void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
/**
* Process tuples falling within the window and optionally emit 
* new tuples based on the tuples in the input window.
*/
void execute(TupleWindow inputWindow);
void cleanup();
}
```

每当窗口滑动一次，execute方法就是被调用一次。保证节点处理的窗口一定是当前窗口，当前窗口包含的数据都是当前应该被计算的Tuples。Bolt compoent除了需要继承IWindowedBolt接口外，还需要继承BaseWindowedBolt，该class定义了创建窗口的api函数：

```
/* 
 * Tuple count based sliding window that slides after slidingInterval number of tuples 
 */
withWindow(Count windowLength, Count slidingInterval)
/* 
 * Tuple count based window that slides with every incoming tuple 
 */
withWindow(Count windowLength)
/* 
 * Tuple count based sliding window that slides after slidingInterval time duration 
 */
withWindow(Count windowLength, Duration slidingInterval)
/* 
 * Time duration based sliding window that slides after slidingInterval time duration 
 */
withWindow(Duration windowLength, Duration slidingInterval)
/* 
 * Time duration based window that slides with every incoming tuple 
 */
withWindow(Duration windowLength)
/* 
 * Time duration based sliding window that slides after slidingInterval number of tuples 
 */
withWindow(Duration windowLength, Count slidingInterval)
/* 
 * Count based tumbling window that tumbles after the specified count of tuples 
 */
withTumblingWindow(BaseWindowedBolt.Count count)
/* 
 * Time duration based tumbling window that tumbles after the specified time duration 
 */
withTumblingWindow(BaseWindowedBolt.Duration duration)
```

用户自己创建的bolt只要继续上述窗口class后，其他api都不需要变动什么，只要将原先以一个tuple为单位的处理逻辑改成以一个窗口为单位的处理逻辑就可以了。

```
public class SlidingWindowBolt extends BaseWindowedBolt {
   private OutputCollector collector;
   @Override
   public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
       this.collector = collector;
   }
   @Override
   public void execute(TupleWindow inputWindow) {
     for(Tuple tuple: inputWindow.get()) {
       // do the windowing computation
       ...
     }
     collector.emit(new Values(computedValue));
   }
}
```

## Tuple timestamp 

在窗口执行过程中每个tuple对应一个时间戳，默认情况下Window中涉及的时间都是指当前消息处理时间，这时候每个tuple对应的时间戳是通过System.currentTime()生成的；但Storm/Jstorm 以支持以消息本身的时间（event time）来定义窗口、执行窗口操作，这时候每个tuple必须携带一个时间戳，且消息携带的时间戳必须以预设好的字段来标识。比如我们在创建topology时，通过api设置好这个字段：

```
/**
* Specify the tuple field that represents the timestamp as a long value. If this field
* is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
*
* @param fieldName the name of the field that contains the timestamp
*/
public BaseWindowedBolt withTimestampField(String fieldName)
```

比如我们设置进去的是“ts”。那么在core里头会通过调用

```
long ts = input.getLongByField("ts")
```

来获取消息本身的时间戳。


## Watermarks
当用户的处理节点是通过基于Event Time的时间窗口来处理数据时，它必须在确定所有属于该时间窗口的消息全部流入操作节点后才能开始数据处理。但是由于消息可能是乱序的，所以操作节点无法直接确认何时所有属于该时间窗口的消息全部流入此操作符。这时候storm以和flink一样，引入了WaterMark的概念。WaterMark本身也是一条消息，包含一个时间戳，Storm使用WaterMark标记所有小于该时间戳的消息都已流入，Storm会会根据已经流入该节点的消息定时生成一个包含该时间戳的WaterMark，插入到消息流中输出到Storm流处理系统中，Storm操作符按照时间窗口缓存所有流入的消息，当bolt节点处理到     WaterMark时，它对所有小于该WaterMark时间戳的时间窗口数据进行处理并发送到下一个处理节点。

为了保证能够处理所有属于某个时间窗口的消息，bolt节点必须等到大于这个时间窗口的WaterMark之后才能开始对该时间窗口的消息进行处理，相对于基于消息处理时间的时间窗口，基于event time的窗口需要占用更多内存，且会直接影响消息处理的延迟时间。对此，一个可能的优化措施是，对于聚合类的操作，我们建议提前对部分消息进行聚合操作，当有属于该时间窗口的新消息流入时，基于之前的部分聚合结果继续计算，这样的话，只需缓存中间计算结果即可，无需缓存该时间窗口的所有消息。

storm在确认收到了上游消息的前提下，会定时（默认是1s）产生一条WaterMark消息。这个定时器可以通过以下api配置：

```
/**
* Specify the watermark event generation interval. Watermark events
* are used to track the progress of time
*
* @param interval the interval at which watermark events are generated
*/
public BaseWindowedBolt withWatermarkInterval(Duration interval)
```
 
WaterMark中时间戳生成是根据bolt节点中已经上游各个流最新的tuple的最小时间戳减去lag。这里的lag我们可以理解为允许消息乱序的程度。比如我们的waterMark时间戳是 09:00:00, lag设置为10秒。如果一条消息过来，他的时间戳大于09:00:10，那么这说明乱序超过了容忍范围，这时候这条消息会被丢到。如果来的一条消息本身的时间戳时候09:00:05，说明这条消息不会被处理，但是会被缓存，等到下一条WaterMark生成之后，才会被处理。lag可以调用以下api进行配置：

```
/**
* Specify the maximum time lag of the tuple timestamp in millis. The tuple timestamps
* cannot be out of order by more than this amount.
*
* @param duration the max lag duration
*/
public BaseWindowedBolt withLag(Duration duration)
```


首先我们需要再次明确一点，一旦bolt节点接到一条WaterMark之后，他会将缓存在节点内部小于这个waterMark时间戳的窗口都进行处理。我们这里举一个例子来说明Storm/JStorm 是怎么利用WaterMark的概念来处理消息乱序的问题：

定义一个Window： Window length = 20s（窗口大小）, sliding interval = 10s（滑动间隔）, watermark emit frequency = 1s（发送频率）, max lag = 5s（容忍的乱序程度, 当前的时间是 = 09:00:00

Tuples e1(6:00:03), e2(6:00:05), e3(6:00:07), e4(6:00:18), e5(6:00:26), e6(6:00:36) 在 9:00:00 and 9:00:01期间被bolt节点接收到

这时09:00:01会定时发送一条waterMark消息，该消息的时间戳为 watermark w1 = 6:00:31，这个时间戳计算是当前流最新的tuple时间戳-lag，即6:00:36-5=6:00:31.由于这时候bolt节点接收到了09:00:01发送的waterMark。这时候bolt节点会开始计算所有小于该时间戳的窗口，即3个窗口：

```
   w1  5:59:50 - 06:00:10 with tuples e1, e2, e3
   w2  6:00:00 - 06:00:20 with tuples e1, e2, e3, e4
   w3  6:00:10 - 06:00:30 with tuples e4, e5
```

e6不在这个waterMark处理，被暂时缓存起来，等待下一个waterMark接收时处理。接下来：

Tuples e7(8:00:25), e8(8:00:26), e9(8:00:27), e10(8:00:39) 在 9:00:01 and 9:00:02 期间被bolt节点接收到

这时09:00:02会定时发送一条waterMark消息，该消息的时间戳为 watermark w1 = 6:00:34.由于这时候bolt节点接收到了09:00:02发送的waterMark。这时候bolt节点会开始计算所有小于该时间戳的窗口，即3个窗口：

```
   w4 6:00:20 - 06:00:40 with tuples e5, e6 (from earlier batch)
   w5 6:00:30 - 06:00:50 with tuple e6 (from earlier batch)。
   w6 8:00:10 - 08:00:30 with tuples e7, e8, e9
```

e10 不在这个waterMark处理，被暂时缓存起来，等待下一个waterMark接收时处理。

## Window 状态管理

     由于window处理节点的executor处理消息是以一个窗口为维度，如果其中窗口中的一条tuple处理失败，可能就会造成整个窗口数据replay。所以storm做了状态管理在，支持将处理window内每一个元素计算完，做状态备份。一旦窗口处理失败，可以从上一次成功处理的状态恢复。这一点如果大家感兴趣可以去了解下。Jstorm目前每个Topology有个TopologyMaster，可以复制整个拓扑的状态管理，所以   Jstorm目前做的状态管理机制比storm好太多了。而且这套状态管理可以很好的支持exactly once/at least once，性能也很赞。所以目前基于window的状态管理我们不推荐利用storm的这套机制。所以这里就不介绍了。后面我们Jstorm的状态管理也会支持对window api的支持。

