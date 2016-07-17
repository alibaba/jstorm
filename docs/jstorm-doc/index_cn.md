---
title: Performance
layout: plain_cn
---
# JStorm 性能
> JStorm 0.9.0 性能非常的好， 使用Netty时单worker发送最大速度为11万QPS(Query Per Second)， 使用zeromq时，最大速度为12万QPS。

## 结论
* JStorm 0.9.0 在使用Netty的情况下，比Storm 0.9.0 使用Netty情况下，快10%， 并且JStorm Netty是稳定的而Storm的Netty是不稳定的
* 在使用ZeroMQ的情况下， JStorm 0.9.0 比Storm 0.9.0 快30%

## 原因
* Zeromq减少一次内存拷贝
* 增加反序列化线程
* 重写采样代码，大幅减少采样影响
* 优化ack代码
* 优化缓冲map性能
* Java比Clojure更底层

## 测试

### 测试样例
测试样例为[https://github.com/longdafeng/storm-examples](https://github.com/longdafeng/storm-examples)

### 测试环境
5台 16核， 98G物理机

```
uname -a :
Linux dwcache1 2.6.32-220.23.1.tb735.el5.x86_64 #1 SMP Tue Aug 14 16:03:04 CST 2012 x86_64 x86_64 x86_64 GNU/Linux
```

### 测试结果
* JStorm with Netty，Spout发送QPS为11万
<center>
  <img src="{{site.baseurl}}/img/performance_cn/jstorm.0.9.0.netty.jpg" width="960px">
  jstorm.0.9.0.netty
</center>

* Storm with Netty, Spout应用发送QPS为10万（截图为上层应用的QPS， 没有包括发送到ack的QPS， Spout发送QPS 正好为上层应用QPS的2倍）
<center>
  <img src="{{site.baseurl}}/img/performance_cn/storm.0.9.0.netty.jpg" width="960px">
  storm.0.9.0.netty
</center>

* JStorm with ZeroMQ, Spout发送QPS为12万
<center>
  <img src="{{site.baseurl}}/img/performance_cn/jstorm.0.9.0.zmq.jpg" width="960px">
  jstorm.0.9.0.zmq
</center>




* Storm with ZeroMQ, Spout 发送QPS为9万（截图为上层应用的QPS， 没有包括发送到ack的QPS， Spout发送QPS 正好为上层应用QPS的2倍）
<center>
  <img src="{{site.baseurl}}/img/performance_cn/storm.0.9.0.zmq.jpg" width="960px">  
  storm.0.9.0.zmq
</center>