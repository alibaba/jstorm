---
title: 从JStorm0.9.x升级到JStorm2.1.1
is_beta: false

sub-nav-group: Upgrade_cn
sub-nav-id: UpgradeApplication_cn
sub-nav-pos: 3
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


# 概述
本文档主要介绍 将0.9.x 的应用升级到2.x。 对于非这种情况下，只需将pom.xml 中jstorm的版本升级到正确的版本，重新编译打包即可。

但是JStorm 2.1.1并不完全向后兼容（主要是thrift版本和metrics接口），从JStorm 0.9.x迁移到JStorm 2.x需要做以下改动：


## 2.x 简述
JStorm 2.x系列是一个全新的版本，它拥有0.9.x的所有feature，同时相比0.9.x系列，还有以下独有的feature：

1. 更好的性能：jstorm 2.1.1对jstorm内核以及metrics进行了大符的性能优化，在我们的性能测试中，2.1.1在最差情况下，性能比0.9.8要好5%~10%。

2. 非常方便自定义监控， metrics底层完全重构，从新的web ui（koala）中我们可以监控所有metrics的历史曲线数据，而不像0.9.x系列版本，只有单点数据。同时非常方便地支持用户自定义metrics，使用基本上跟0.9.x一样的自定义metrics代码，即可在koala上面看到完整的metrics曲线。

3. 支持日志搜索：同时支持topology级别的日志搜索（会搜索一个topology下所有worker的日志）和单文件级别的日志搜索。

4. jstorm 2.1.1支持jdk 1.8。



# 集群重新安装

见：[集群升级]({{site.baseurl}}/QuickStart_cn/Upgrade/UpgradeCluster.html)


# 代码修改：

a. 所有依赖，原来的jstorm依赖全部删除，包括jstorm-client, jstorm-server, jstorm-client-extension。

b. 添加jstorm-core依赖：

```xml
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.1.1</version>
    <scope>provided</scope>
</dependency>
```
 其中2.1.1 换成集群中jstorm的版本，即可

c. 重新编译。这一步可能会出错，因为如果你用了自定义metrics，很可能在2.1.1中，package名或者接口名都已经发生了变化，你需要把原来的package依赖删了，重新import一下。

# 关于日志框架【重要】
和0.9.x系列使用log4j不同，jstorm 2.x默认使用了logback作为日志框架。

## 使用logback日志框架
logback在一般使用时是兼容log4j的，也就是说log4j可以直接桥接到logback，具体为：

a. 添加slf4j-api, log4j-over-slf4j和logback依赖（其实加了logback依赖之后就不需要加slf4j-api依赖了），具体：

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

b. 排除pom中所有的slf4j-log4j12的依赖，因为slf4j-log4j12跟log4j-over-slf4j是冲突的：

```
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.5</version>
    <scope>provided</scope>
</dependency>
```

这里版本一般是1.7.5，但是还要具体看你的应用pom仲裁出的版本。

理论上，这样就能够把log4j桥接到slf4j。

## 使用log4j日志框架

但是，**如果应用代码中动态修改log4j的Pattern，Appender的方式，就会有问题**。

这种情况下，没有什么选择，只能回退到log4j。具体做法跟桥接到logback有点相反：

a. 删除所有log4j-over-slf4j有依赖（如果有），删除logback的依赖，然后加入slf4j-log4j12的依赖和log4j的依赖。

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

b. 除了pom的修改，还需要在代码中，在build topology时添加一行：

```
ConfigExtension.setUserDefinedLog4jConf(conf, "jstorm.log4j.properties");
```

这行代码告诉jstorm使用log4j，并且把jstorm自带的jstorm.log4j.properties作为log4j的配置（当然用户也可以使用其他的log4j配置，此时需要将自定义的log4j配置放在你的jar包的classpath下，并且注意不要用"jstorm.log4j.properties"这种跟jstorm框架自带的配置重名的文件名）。

或者也可以使用配置的方式，在你的配置文件中增加一行：

```
user.defined.log4j.conf: jstorm.log4j.properties
```

效果是一样的（注意如果是配置的话，需要保证你自己的应用会解析这个值，并且将它put到提交时的conf Map中）。


另外，如果你想设置你的集群默认对所有topology都使用log4j，而不是logback，那么你可以在storm.yaml中添加上面这行配置，然后把这个storm.yaml复制到所有supervisor中。

当然这样的代价是，如果你需要使用logback，则需要手动在你的应用中添加一行配置：

```
user.defined.log4j.conf: 
```
 
把log4j的配置重置掉。这样它就会使用logback了。


# 自定义metrics使用指南 
使用MetricClient来注册自定义metrics。目前支持以下几种metrics，请按需使用：

* COUNTER：计数器，如jstorm中的emitted, acked, failed都使用counter。

* METER：一般用于qps/tps的统计。jstorm中的sendTps, recvTps使用meter。

* GAUGE：一般用于统计一个瞬时值，如memUsed, cpuRatio，queueSize等这种瞬间的值。**但是需要注意，gauge是不可聚合的**，即如果你注册了一个task级别的gauge，你只能到koala web ui的task页面查看它的metrics，它并不会聚合到component级别，因为这种聚合没有实际意义（想象一下多个task的queueSize加一起是什么意思...）。

* HISTOGRAM：一般用于统计操作的耗时，如EmitTime，nextTupleTime等。

参考代码见example中sequence-split-merge工程TotalCount类。

