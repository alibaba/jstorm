---
title: Ack Framework
layout: plain
top-nav-title: Ack Framework
top-nav-group: advance
top-nav-pos: 3
sub-nav-title: Ack Framework
sub-nav-group: advance
sub-nav-pos: 3
---
The tracking algorithm of acker is one of the major breakthroughs of Storm. JStorm extends it and makes some code optimization.

## Usage Scenarios
By the help of acker framework, the developer can well know the exact status of every single message that a spout has sent, either successful or failed. For instance, when integrated with the MetaQ or Kafka, if the message is successfully handled, the offset can be updated to the next, otherwise, resend this message.
Therefore, it's very easy to guarantee all the data will be handled without a miss via acker framework.

> _Note_:<br>
    A failed tuple will not be resent automatically. It only triggers the "fail" logic of the spout, which needs to be implemented by the developer. By default, it will do nothing. You may need to implement the reload and send operation.

JStorm provide [two interfaces](https://github.com/alibaba/jstorm/tree/master/jstorm-client-extension/src/main/java/com/alibaba/jstorm/client/spout) to easily get tuple's value in ack or fail. So it is easily to resent data in fail, update Kafka/Rocket-MQ offset in ack operation.


In the acker framework, for each tuple a spout has sent:
* If the spout receives the response from the acker in the given time, we say this tuple is handled successfully
* If the spout hasn't received the response till timeout, and the fail logic will be triggered, we say this tuple is failed.
* The fail response from the acker also means the fail of the tuple.

Additionally, ack framework can also be used to do flow control. Sometimes, the spout sending tuples' speed is faster than the bolt handling speed. To slow down the spout, we could control the number of max pending(no ack or fail response in time) tuples to make the spout not to execute nextTuple, when the actual number exceeds this configuration.
Set the max pending tuples of spout: `conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, pending)`;

## How to use the ack framework
* Append the msgId when emit the data in spout
* Set the number of acker over 0: `Config.setNumAckers(conf, ackerParal)`;
* Remember to ack the tuple in the bolt:
    * _Bolt_ implements _IRichBolt_ has to call the ack(`OutputCollector.ack(tuple)`) or fail(`OutputCollector.fail(tuple)`) method manually. 
    * _Bolt_ implements _IBasicBolt_ will ack automatically unless a _FailedException_ is throwed.

## How to close the ack function
Two way:
* No msgId when emitting data in the spout
* Set the number of acker as 0

## Difference with Storm
* Only do one Map insert operation in acker to improve acker performance
* Spout guarantee Tuple's message rootId is unique to avoid missing track one tuple
* When the number of acker isn't 0, bolt should call Collector.ack(tuple) or Collector.fail(tuple) after execute one tuple. Because all metrics will be collected only when bolt execute Collector.ack or Collector.fail when acker isn't 0.


## Implementation
Please refer to [Storm Acker](http://storm.apache.org/documentation/Acking-framework-implementation.html)