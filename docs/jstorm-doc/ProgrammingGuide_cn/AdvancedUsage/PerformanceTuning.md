---
title:  "性能优化"
layout: plain_cn

#sub-nav-parent: AdvancedUsage
sub-nav-group: AdvancedUsage_cn
sub-nav-id: PerformanceTuning_cn
sub-nav-pos: 10
sub-nav-title: 性能优化
---

* This will be replaced by the TOC
{:toc}

在对性能优化前，必须仔细阅读 [开发经验总结](https://github.com/alibaba/jstorm/wiki/%E5%BC%80%E5%8F%91%E7%BB%8F%E9%AA%8C%E6%80%BB%E7%BB%93)
[运维经验总结](https://github.com/alibaba/jstorm/wiki/%E8%BF%90%E7%BB%B4%E7%BB%8F%E9%AA%8C%E6%80%BB%E7%BB%93)

这2篇文章中，有很多性能优化相关的经验。

# 选型 
按照性能来说， trident < transaction < 使用ack机制普通接口 < 关掉ack机制的普通接口， 因此，首先要权衡一下应该选用什么方式来完成任务。

如果“使用ack机制普通接口”时， 可以尝试关掉ack机制，查看性能如何，如果性能有大幅提升，则预示着瓶颈不在spout， 有可能是Acker的并发少了，或者业务处理逻辑慢了。

# 增加并发
可以简单增加并发，查看是否能够增加处理能力

# 让task分配更均匀
当使用fieldGrouping方式时，有可能造成有的task任务重，有的task任务轻，因此让整个数据流变慢， 尽量让task之间压力均匀。

# 使用MetaQ或Kafka时
对于MetaQ和Kafka， 一个分区只能一个线程消费，因此有可能简单的增加并发无法解决问题， 可以尝试增加MetaQ和Kafka的分区数。