---
title:  "How to create/remove/enlarge/reduce one JStorm logic cluster on yarn?"
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


## 创建集群
* 创建一个集群， 限制这个集群的资源数（cpu核和内存）
* 每个集群，可以支持不同的jstorm版本和jdk版本
* 一台机器上可以运行多个逻辑集群的container
* 启动方式:执行submitJstormOnYarn命令提交集群

## 销毁集群
* 执行jstormyarn killJstormOnYarn，结束一个集群的进程
## 升级集群binary
* 更新deploy目录下的jstorm部署文件
* 执行upgradeCluster命令

## 升级集群配置
* 更新deploy/jstorm/conf下的storm.yaml
* 执行upgradeCluster命令

## 下载集群的binary和配置文件
* 配置项jstorm.yarn.instance.deploy.dir是当前binary文件在hadoop上的存放路径

## 重启容器内进程
* 执行upgradeCluster命令

## 查看集群的状态
* 执行info命令

## 对集群进行伸缩容
### 随机分配容器
* 执行addSupervisors和removeSupervisors命令

### 指定机架
* 执行addSpecSupervisors和removeSpecSupervisors命令

# JStorm-on-Yarn流程
jstorm-on-yarn采取批发模式，对应于spark-on-yarn的零售模式，也就是说每次往spark集群提交一个application时，都会生成一个AM，而jstorm-on-yarn的AM是常驻的。

批发模式比零售模式提高了50%的资源利用率，例如我们发现，在实际场景中，可能零售模式(spark-on-yarn)最多只能使用60-70%的物理资源，但批发模式能够达到90%，这个差距是相当大的；并且灵活的容器粒度给业务方的支持会好很多，比如会有这样的情况，应用1在A时段使用内存较多，B时段使用CPU较多。应用2在A时段使用网络IO较多，B时段使用内存较多，那么若按照零售模式分配资源，总资源量相当于应用在每个资源维度的峰值集合。而批发模式可以满足多个应用需要share一些资源(CPU，内存，网络IO)的情况，能够减少非常多的资源浪费；零售模式下容器的crash次数很多，这是由于在默认的GC模式下，大部分应用可能比较少触发老年区GC，这样内存的消耗会随着时间推移慢慢逼近上限，RM会直接释放容器，杀死进程，AM不得不重新选择一个新的容器来执行，这样不仅会增加调度时间，还需要额外的状态管理,但在批发模式中，由于优化了GC策略以及从根本上更好的粒度支持，是完全能够避免这类情况发生的。

因此目前jstorm-on-yarn的工作方式是，每个集群有一个总控的AM，它负责以下工作：

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

若需要触发nimbus自动重启，还需要添加如下配置
```
blobstore.dir: /yourhdfsdir
blobstore.hdfs.hostname: yourhdfshost
blobstore.hdfs.port: yourhdfsport
```




## Yarn 配置

TO BE ADDED




