---
title: Ack 机制
layout: plain_cn
top-nav-title: Ack 机制
top-nav-group: 进阶
top-nav-pos: 3
sub-nav-title: Ack 机制
sub-nav-group: 进阶
sub-nav-pos: 3
---
ack 机制是storm整个技术体系中非常闪亮的一个创新点， JStorm很好的继承了这个机制，并对原生storm的ack机制做了一点点代码优化。

## 应用场景
通过Ack机制，spout发送出去的每一条消息，都可以确定是被成功处理或失败处理， 从而可以让开发者采取动作。比如在Meta中，成功被处理，即可更新偏移量，当失败时，重复发送数据。

因此，**通过Ack机制，很容易做到保证所有数据均被处理，一条都不漏。**

另外需要注意的，**当spout触发fail动作时，不会自动重发失败的tuple，需要spout自己重新获取数据，手动重新再发送一次**

ack机制即， spout发送的每一条消息，

* 在规定的时间内，spout收到Acker的ack响应，即认为该tuple 被后续bolt成功处理
* 在规定的时间内，没有收到Acker的ack响应tuple，就触发fail动作，即认为该tuple处理失败，
* 或者收到Acker发送的fail响应tuple，也认为失败，触发fail动作

另外Ack机制还常用于限流作用：
为了避免spout发送数据太快，而bolt处理太慢，常常设置pending数，当spout有等于或超过pending数的tuple没有收到ack或fail响应时，跳过执行nextTuple， 从而限制spout发送数据。

通过`conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, pending);`设置spout pend数。

## 如何使用Ack机制
* spout 在发送数据的时候带上msgid
* 设置acker数至少大于0；`Config.setNumAckers(conf, ackerParal);`
* 在bolt中完成处理tuple时，执行OutputCollector.ack(tuple), 当失败处理时，执行OutputCollector.fail(tuple);
** 推荐使用IBasicBolt， 因为IBasicBolt 自动封装了OutputCollector.ack(tuple), 处理失败时，请抛出FailedException，则自动执行OutputCollector.fail(tuple)

## 如何关闭Ack机制
有2种途径

* spout发送数据是不带上msgid
* 设置acker数等于0


#Acker的原理：
acker的原理如下图所示：

![acker]({{site.baseurl}}/img/advance_cn/ack/acker.jpg)

当Acker中rootid的value为0时，就发送ack 响应给spout，如果直接收到fail消息，则发送fail响应给spout