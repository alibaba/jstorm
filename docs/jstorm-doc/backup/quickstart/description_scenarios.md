---
title: Description & Scenarios
layout: plain
top-nav-title: Description & Scenarios
top-nav-group: quickstart
top-nav-pos: 1
sub-nav-title: Description & Scenarios
sub-nav-group: quickstart
sub-nav-pos: 1
---
## JStorm is a distributed real-time computation engine.

JStorm is a distributed and fault-tolerant realtime computation system. Inspired by Apache Storm, JStorm is completely implemented from scratch in Java, and provides many features which are much more enhanced. JStorm has been widely used in many enterprise environments and proved robust and stable.

JStorm provides a distributed programming framework very similar to Hadoop MapReduce. The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API (which is fully compatible with Apache Storm API) and submit the composed "Topology" to a working JStorm instance.

Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph). Different from Hadoop MapReduce, a JStorm topology runs 24 * 7, the very nature of its continuity and 100% "in-memory" architecture has been proved a particularly suitable solution for streaming data and real-time computation.

JStorm guarantees fault-tolerance. Whenever a worker process crashes, the scheduler embedded in the JStorm instance immediately spawns a new worker process to  take the place of the failed one. The "Acking" framework provided by JStorm guarantees that every single piece of data will be processed at least once.

## Advantages
There are many real-time computation engines before , but the Storm and JStorm have become more and more popular since they appeared. The advantages are listed below:

* Quick start; The programming interface is easy to learn. To develop a scalable  application,  without thinking about the underlying rpc, redundancy between workers, data distribution, developers just need to observe the programming specifications of Topology, Spout and Bolt. 

* Perfect scalability; Performance can be improved linearly when component parallelism increased. 

* Robust. The scheduler is able to automatically assign a new worker to replace unavailable workers in case of process or machine failures. 

* Accuracy of the data. Acker mechanism can prevent data from been lost. If there are more steps on the accuracy requirements, use transaction mechanism to ensure data accuracy. 

## Scenarios
JStorm processes data via the message processing pipeline , which is particularly suitable for stateless calculation. That is, the data to be calculated should be contained in the received messages, and preferably a data stream does not depend on another data stream. Therefore, it is often used like:

* Log analysis. JStorm can analyze specific data from the log, and store the analysis results in an external storage system such as a database. Currently, most of log analysis system is basing on JStorm or Storm.

* Pipeline system. JStorm can transfer data from one system to another, such as synchronizing data to Hadoop.

* Message converter. According to a certain format, JStorm can convert the messages received, and then store them into another system, such as a messaging middleware.

* Statistical analyzer. JStrom can extract a certain field from the logs or messages, then calculates to count or sum the data and finally stores the statistics into the external storage. The intermediate processing may be more complicated.
