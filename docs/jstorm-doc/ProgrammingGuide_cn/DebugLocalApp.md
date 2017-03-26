---
title:  "How to debug local topology?"
# Top-level navigation
top-nav-group: ProgrammingGuide_cn
top-nav-pos: 1
top-nav-id: DebugLocal_cn
top-nav-title: 本地调试
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


# 如何本地调试 JStorm 程序？

JStorm 提供了两种运行模式：本地模式和分布式模式。本地模式针对开发调试storm topologies非常有用。

如果你还在用日常的web ui提交拓扑这种远古的方式进行调试测试，那就赶快阅读本文吧。本文将介绍在本机不安装JStorm环境的情况下，开发、调试JStorm程序。

## 单机模式主要是在代码中加入：

	```java
	import backtype.storm.LocalCluster;
	
	LocalCluster cluster = new LocalCluster();
	
	//建议加上这行，使得每个bolt/spout的并发度都为1
	conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);

	//提交拓扑
	cluster.submitTopology("SequenceTest", conf, builder.createTopology());
	
	//等待1分钟， 1分钟后会停止拓扑和集群， 视调试情况可增大该数值
	Thread.sleep(60000);		
			
	//结束拓扑
	cluster.killTopology("SequenceTest");
	
	cluster.shutdown();
	```
	用LocalCluster来模拟集群环境，你可以在`LocalCluster`对象上调用`submitTopology`方法来提交拓扑，`submitTopology(String topologyName, Map conf, StormTopology topology)`接受一个拓扑名称，一个拓扑的配置，以及一个拓扑的对象。就像`StormSubmitter`一样。你还可以调用`killTopology`来结束一个拓扑。对应的还有`active`,`deactive`,`rebalance`等方法。由于JStorm是个不会停止的程序，所以我们最后需要显示地停掉集群。

## 修改pom.xml

	以jstorm 2.2.0版本为例。
	
	```xml
	<dependency>
	  <groupId>com.alibaba.jstorm</groupId>
	  <artifactId>jstorm-core</artifactId>
	  <version>2.2.1</version>
	  <!-- keep jstorm out of the jar-with-dependencies -->
	  <!-- <scope>provided</scope> -->
	</dependency>
	```

	注意要注释掉jstorm依赖中的`<scope>provided</scope>`，**而提交的时候必须记得将这行改回来！** 否则会报多个`defaults.yaml`的错误。(有的IDE 使用`<scope>provided</scope>` 是可以支持本地模式， 有的IDE 不支持， 为了安全，可以先注释掉)
	
	*注：如果依赖的是 0.9.x 版本的jstorm，会有三个依赖包，将这三个依赖的provided都注释掉。*

## 运行main class。

	运行后的截图如下：
![image]({{site.baseurl}}/img/programguide/localdebug.jpg)

	为了更好的代码组织，建议将本地运行和集群运行写成两个方法，根据参数/配置来调用不同的运行方式。更多可以参照[SequenceTopology的例子](https://github.com/alibaba/jstorm/blob/master/example/sequence-split-merge/src/main/java/com/alipay/dw/jstorm/example/sequence/SequenceTopology.java)

## 注意点

	本地调试主要是用于测试应用逻辑的，因此有一些限制，如classloader是不起作用的。此外，还需要注意一下你的应用中log4j的依赖，如果应用的依赖中自带了log4j.properties，则有可能导致将jstorm默认的本地测试的log4j配置覆盖掉，从而导致调试时控制台没有任何输出。

## 常见问题：

### 控制台没有任何输出
有几个原因可能导致这个问题：

    1.如果在2.2.0中依赖了slf4j-log4j12，会跟jstorm自带的log4j-over-slf4j冲突，需要将slf4j-log4j12排除掉。

    2.确认你打的日志是用slf4j-api打的，即LoggerFacgtory而不是log4j的Logger