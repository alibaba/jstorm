---
title:  "How to create/remove/enlarge/reduce one JStorm logic cluster?"
layout: plain_cn

# Top-level navigation
top-nav-group: Maintenance_cn
top-nav-id: JStormOnYarn_cn
top-nav-pos: 5
top-nav-title: JStorm-on-Yarn 
---

* This will be replaced by the TOC
{:toc}


# 功能点
目前支持脚本调用的方式调用thrift接口，实现集群管理。
调用路径为bin/JstormYarn

## 创建集群
* 创建一个集群， 限制这个集群的资源数（cpu核和内存）
* 每个集群，可以支持不同的jstorm版本和jdk版本
* 一台机器上可以运行多个逻辑集群的container
* 启动方式:执行submitJstormOnYarn命令提交集群，然后通过startNimbus与addSupervisors命令启动nimbus与supervisor

## 销毁一个集群
* 执行killJstormOnYarn，结束进程

## 升级一个集群binary
* 更新deploy目录下的jstorm部署文件
* 执行upgradeCluster命令

## 升级一个集群配置
* 更新deploy/jstorm/conf下的storm.yaml
* 执行upgradeCluster命令

## 下载一个集群的binary和配置文件
* 配置项jstorm.yarn.instance.deploy.dir是当前binary文件在hadoop上的存放路径

## 重启集群
* 执行upgradeCluster命令

## 查看集群的状态
* 执行list命令

## 对集群进行伸缩容
* 执行addSupervisors和removeSupervisors命令

# JStorm-on-Yarn流程
不同于spark-on-yarn，每次往spark集群提交一个application时，都会生成一个AM，jstorm-on-yarn的AM是常驻的，也就是说，对于一个JStorm集群，
只会有一个AM。

这点主要是由JStorm的调度方式决定的：JStorm目前已经有了TopologyMaster，作为topology的仲裁者。按照YARN的设计，最理想的应该是把
TopologyMaster作为AM，来协调一个具体topology的运行，包括worker的分配，task心跳的跟踪等。但是因为目前TopologyMaster已经成形，而且
在它之上，metrics、心跳、反压都会经过TM，这就导致如果修改的话，成本就会非常大，可能导致整个JStorm架构的变化，有些得不偿失了。

因此目前jstorm-on-yarn的工作方式是，有一个总控的AM，它负责以下工作：

* 创建Nimbus（并在nimbus挂掉的时候自动重启）
* 接收请求创建Supervisor
* 集群的动态扩容、缩容

而提交、杀死等对topology的操作，则跟standalone的JStorm集群一样，直接跟container中的nimbus和supervisor交互，并不跟AM交互。

## 创建AM
从start-JstormYarn.sh开始，它会调用JStormOnYarn类来创建AM，本质上是JStorm的yarn client。
这个类比较简单，主要就是设置nimbus容器的一些参数，如内存，CPU core，jar，libjar，shellscript（实际值为start_jstorm.sh，用于启动nimbus和supervisor）等。
然后把jar、shellscript这些上传到HDFS中。最后调用`yarnClient.submitApplication(appContext)`创建AM。

## 创建nimbus和supervisor
这一步是通过thrift接口来实现的，具体实现为：client端JstormAM.py（thrift自动生成），server端自动生成的RPC接口为JstormAM，而真正的处理
逻辑为JstormAMHandler类（跟jstorm中的thrift是一样的）。这个handler在上面创建AM的时候就会初始化好thrift server，监听client的请求了。

来看几个比较重要的方法：addSupervisors和startNimbus
这两个方法其实实现差不多，本质上就是指定container的数量、CPU核数。具体起supervisor、nimbus还是在JStormMaster中做的。它会根据container的
priority属性来决定是起supervisor还是nimbus。

需要注意的是，jstorm-on-yarn创建supervisor，并不是只申请supervisor本身的内存，而是会指定一大块的内存（比如20或40G），这个container会
容纳该supervisor下的所有worker。当容器有问题挂掉时，所有的worker也会被杀死，这比standalone下的集群更为严格一些。

# 运维和配置说明

## jstorm配置
为了运维方便，请在所有的YARN集群的storm.yaml中添加以下配置：

```
 jstorm.on.yarn: true
 supervisor.childopts:  -Djstorm-on-yarn=true
 jstorm.log.dir: /home/yarn/jstorm_logs/<cluster_name>
```
以上配置会覆盖默认的日志路径，将所有的日志都重定向到/home/yarn下面，防止container挂掉之后出现日志找不到的情况。如果同一台机器上起了多个supervisor的container，会在supervisor日志后面加上`supervisor.deamon.logview.port`的值（YARN会为每个container动态生成端口及端口区间）以区分，防止串日志。如两个supervisor的http端口分别为9000和9001，则会生成supervisor-9000.log和supervisor-9001.log。koala会根据`jstorm.on.yarn`配置为日志加上指定的端口后缀。


## Yarn 配置

TO BE ADDED


# YARN入门系列文章
见：http://zh.hortonworks.com/get-started/yarn/


