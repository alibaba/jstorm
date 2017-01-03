---
title: Upgrade JStorm0.9.x To JStorm2.1.1
is_beta: false

sub-nav-group: Upgrade
sub-nav-id: UpgradeApplication
sub-nav-pos: 3
---

* This will be replaced by the TOC
{:toc}

JStorm 2.x are new version series, they have all features of 0.9.x, besides, they have these additional features:

1. Better performance: jstorm 2.1.1 did a lot of optimization to JStorm core and metrics. In our performance tests, JStorm 2.1.1 is at least 10% faster than JStorm 0.9.8 in the worst case.

2. Redesigned metrics core, from web ui(koala, not open source yet) we can see all history metrics instead of single static points in 0.9.x. It's also very easy to use user-defined metrics, with a few lines of code, users can see history metrics in web ui. 

3. Minimum 1-min window metrics ensures more accurate metrics data.

4. Supports simple log search and topology log search, both forward and backward search are supported.

5. JStorm 2.1.1 supports jdk 1.8

But JStorm 2.1.1 is not quite backward compatible to JStorm 0.9x, mainly because changes to thrift/thrift dependency version and metrics client, the following steps are required to migrate JStorm 0.9.x to JStorm 2.1.1:

### 1.Upgrade your cluster

See: [Upgrade Guide]({{site.baseurl}}/quickstart/upgrade)

### 2.Code changes

a. Remove all old JStorm dependencies in your pom, including jstorm-client, jstorm-server, jstorm-client-extension.

b. Add jstorm-core dependency:

```xml
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.1.1</version>
</dependency>
```

c. Rebuild your code, there might be errors in this step when you used user-defined metrics and MetricsClient since in JStorm 2.1.1 packages are changed, you may need to delete old packages and reimport the classes.

### 3. About logging frameworks [Very Important]
JStorm 2.1.1 has switched to logback instead of log4j in JStorm 0.9.x.

#### Use logback as your logging framework
In most cases, log4j can be bridged to logback, the detailed steps are:

a. add slf4j-api, log4j-over-slf4j and logback dependencies:

```
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.5</version>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>log4j-over-slf4j</artifactId>
    <version>1.7.10</version>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.0.13</version>
</dependency>
```

b. Exclude slf4j-log4j12 in your pom since it conflicts with log4j-over-slf4j:

```
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.5</version>
    <scope>provided</scope>
</dependency>
```

Note that we use `<scope>provided</scope>` to exclude slf4j-log4j12, it's also fine to use `<excludes>`.
Theoretically this should do the work.

#### Use log4j as your logging framework
Above guides you to bridge log4j to logback, but if you use `Pattern, Appender` programmatically in your code, this may not work because some old versions of log4j-over-slf4j don't provide the same class/methods, in this case you may have to use log4j.

The steps are kind of opposite:

a. exclude log4j-over-slf4j dependency(if any), exclude logback dependency, then add slf4j-log4j12 to your pom:

```
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>log4j-over-slf4j</artifactId>
    <version>1.7.10</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.0.13</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.5</version>
</dependency>
<dependency>
    <groupId>log4j</groupId>
    <artifactId>log4j</artifactId>
    <version>1.2.17</version>
</dependency>
```

b. Besides changes in your pom, you also need to add a line of code before calling `TopologyBuilder.buildTopology`: 

```
ConfigExtension.setUserDefinedLog4jConf(conf, "jstorm.log4j.properties");
```

This line tells JStorm to use log4j and use `jstorm.log4j.properties` as log4j config(this file is in ${JSTORM_HOME}/conf), also you can provide your own log4j property file as long as it's in your classpath or anywhere JStorm can find it. 

You can also add this to your conf before submitting your topology.

```
user.defined.log4j.conf: jstorm.log4j.properties
```

In you want to use log4j by default in your cluster rather than logback, you may put the above line into your storm.yaml and sync storm.yaml to all supervisors.

The cost of doing so is that when you want to use logback, you need to add following conf in your topology:

```
user.defined.log4j.conf: 
```

The above conf resets log4j conf thus JStorm will use logback. 

### 4.How to use user-defined metrics
Use MetricClient to register user-defined metrics, the following kinds of metrics are supported:

COUNTER: as the name indicates, it records count like `emitted, acked, failed`.

METER: basically used to calculate qps/tps like `sendTps, recvTps` in JStorm.

GAUGE: used to record an instant value like `memUsed, cpuRatio, queueSize`,etc. **Note that gauges cannot be aggregated**, i.e., if you have registered a task-level gauge, JStorm won't aggregate its value to component-level since it doesn't make any sense.

HISTOGRAM: used for timing, like `EmitTime, nextTupleTime` etc.

You can refer to `TotalCount` class for detailed usage of user-defined metrics and MetricClient in sequence-split-merge project in the example module.
