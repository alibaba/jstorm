---
title:  "Basic Conception"
# Top-level navigation
top-nav-group: QuickStart
top-nav-pos: 1
top-nav-title: "Basic Conception"
---

* This will be replaced by the TOC
{:toc}

#TO BE MODIFY
#the architecture is quite different from the cn version

## Stream ##

JStorm contain the abstraction of stream, which is an ongoing continuous unbounded tuple sequence. Note that the abstract event in stream is tuple when modeling event stream in JStrom, followed when we explain JStorm we will explain how to use it.

![STREAM]({{site.baseurl}}/img/quickstart/conception/stream.jpg)


## Topology ##

![topology]({{site.baseurl}}/img/quickstart/conception/topology.jpg)


According to the introduction all above, we can easily understand this picture, This is a DAG, in JStorm this picture abstracted as a topology(indeed topological structure is a directed acyclic), topology is the highest abstraction in JStorm, it can be submitted to JStorm cluster to be run there. A topology is a data stream conversion chart, each node in graph above represents a spout or bolt, and each edge in the graph represents one data stream. After spout or bolt sends tuple to a stream, it will send tuple to each bolt subscribed from this stream.(which means we do not need to manually pull the stream, as long as pre-subscriptions, spout stream will be sent to the appropriate bolt). Here insert position to talk about the achieve of topology in JStorm, for real-time calculation, we need to design a topology diagram and implement the handle detail of bolt, topology is just some defined thrift structure, so that we can use other languages to create or submit topology.

## Spout/Bolt ##

"Spout" and "Bolt" are two basic primitives Storm provides for stream transformations. They have interfaces to be implemented before you can run your application-specific logic.

JStorm considers that every stream has a source, so sources are abstracted as spout. It may be a source which connect to Messaging Middleware Component([MetaQ](https://github.com/alibaba/RocketMQ-docs), [Kafka](http://kafka.apache.org/), [ActiveMq](activemq.apache.org), TBNotify etc.), and continuously sends out messages, it may be continuously reading from a queue, and emits out tuples which assemblies by the element of the queue.

A bolt consumes any number of input streams, does some processing, and possibly emits new streams. Complex stream transformations, like computing a stream of trending topics from a stream of tweets, require multiple steps and thus multiple bolts. Bolts can do anything from running functions, filtering tuples, streaming aggregations, streaming joins, talking to databases, etc.

Networks of spouts and bolts are packaged into a "topology" which is the top-level abstraction that you submit to Storm clusters for execution. A topology is a graph of stream transformations where each node is a spout or bolt. Edges in the graph indicate which bolts are subscribing to which streams. When a spout or bolt emits a tuple to a stream, it sends the tuple to every bolt that subscribed to that stream.
![spoutbolt]({{site.baseurl}}/img/quickstart/conception/spoutbolt.jpg)

## Tuple ##

The data in stream is abstracted as tuple in JStorm. A tuple is a list of values, each value in the list has a name, and the value can be a basic type, character type, byte array, of course, also can be any other serializable type. Each node in the topology of the field in which it must explain emitted tuple name, other nodes only need to subscribe to the name.

## Worker/Task ##

Worker and Task are the execution units in JStorm, a worker represents a process, a task represents a thread, and tasks run in worker, one worker can run more than one task.

## Resource Slot ##

In JStorm, the resource types are divided into three dimensions, CPU, Memory, and Port, no longer confined to port like Storm. That is, how many CPU Slot, how many Memory Slot, how many Port Slot a supervisor can provide, please refer to [User-Define-Scheduler](https://github.com/alibaba/jstorm/wiki/User-Define-Scheduler) for details.

* A worker consumes a Port Slot.

* Topology can set how many CPU slot will one worker cost. If there are some tasks cost too much cpu, please allocate more CPU slot to them.

* Topology can set how many Memory will be assigned to one worker task. The default size is 2GB. Please enlarge it when not enough.
