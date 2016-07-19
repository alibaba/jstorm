---
title: 开发经验总结
layout: plain_cn
top-nav-title: 开发经验总结
top-nav-group: 进阶
top-nav-pos: 8
sub-nav-title: 开发经验总结
sub-nav-group: 进阶
sub-nav-pos: 8
---
* 在jstorm中， spout中nextTuple和ack/fail运行在不同的线程中， 从而鼓励用户在nextTuple里面执行block的操作， 原生的storm，nextTuple和ack/fail在同一个线程，不允许nextTuple/ack/fail执行任何block的操作，否则就会出现数据超时，但带来的问题是，当没有数据时， 整个spout就不停的在空跑，极大的浪费了cpu， 因此，jstorm更改了storm的spout设计，鼓励用户block操作（比如从队列中take消息），从而节省cpu。
* 在架构上，推荐 “消息中间件 + jstorm + 外部存储” 3架马车式架构
  *  JStorm从消息中间件中取出数据，计算出结果，存储到外部存储上
  *  通常消息中间件推荐使用RocketMQ，Kafka
  *  外部存储推荐使用HBase，Mysql
  *  该架构，非常方便JStorm程序进行重启（如因为增加业务升级程序）
  *  职责清晰化，减少和外部系统的交互，JStorm将计算结果存储到外部存储后，用户的查询就无需访问JStorm中服务进程，查询外部存储即可。
* 在实际计算中，常常发现需要做数据订正，因此在设计整个项目时，需要考虑重跑功能
  *  在meta中，数据最好带时间戳
  *  如果计算结果入hadoop或数据库，最好结果也含有时间戳

* 如果使用异步kafka/meta客户端(listener方式)时，当增加/重启meta时，均需要重启topology
* 如果使用trasaction时，增加kafka/meta时， brokerId要按顺序，即新增机器brokerId要比之前的都要大，这样reassign spout消费brokerId时就不会发生错位。
* 非事务环境中，尽量使用IBasicBolt
* 计算并发度时，
  * spout 按单task每秒500的QPS计算并发
  * 全内存操作的task，按单task 每秒2000个QPS计算并发
  * 有向外部输出结果的task，按外部系统承受能力进行计算并发。
* 对于MetaQ 和 Kafka， 
  * 拉取的频率不要太小，低于100ms时，容易造成MetaQ/Kafka 空转次数偏多
  * 一次获取数据Block大小推荐是2M或1M，太大内存GC压力比较大，太小效率比较低。
* 推荐一个worker运行2个task
* 条件允许时，尽量让程序可以报警，比如某种特殊情况出现时，比如截止到凌晨2点，数据还没有同步到hadoop，发送报警出来
* 从jstorm 0.9.5.1 开始， 底层netty同时支持同步模式和异步模式， 
  * 异步模式， 性能更好， 但容易造成spout 出现fail， 适合在无acker模式下，storm.messaging.netty.sync.mode: false
  * 同步模式， 底层是接收端收一条消息，才能发送一条消息， 适合在有acker模式下，storm.messaging.netty.sync.mode: true

## 常见经验
* 使用zookeeper时， 建议使用curator，但不要使用过高的curator版本
* 数据热点问题

哪位朋友有好的经验愿意分享， 可以发邮件给我们 hustjackie@gmail.com

