---
title:  "How to enable resource isolation?"
layout: plain_cn

# Top-level navigation
top-nav-group: Maintenance_cn
top-nav-id: Isolation_cn
top-nav-pos: 3
top-nav-title: 资源隔离
---

* This will be replaced by the TOC
{:toc}

# 概述
资源隔离是每个计算平台都需要具备的能力， 资源隔离分为几个层次：

* 集群之间
-  * standalone, 直接对集群进行物理隔离部署， 资源隔离效果最理想，缺点是，资源容易浪费
-  * jstorm-on-yarn, jstorm-on-docker,  通过yarn 或docker-swam ， 在一个物理集群上部署多个逻辑集群， 而逻辑集群之间进行资源隔离
* 集群内
-  * 自定义调度， 可以通过jstorm的自定义调度，强制一些任务运行在某些机器上，从而独占这些机器
-  * worker-cgroup,  对worker 进行cgroup设置， 进行cpu和内存进行资源隔离。

本文主要介绍， jstorm的 集群内部的 worker-cgroup 配置方式。

补充一下： 业界常用的2种cgroup 方式，不如jstorm的cgroup方式高效

* 绑核方式， 比如yarn上常用的一种方式，一个worker 绑定到一个cpu 核上， 这种方式最大的问题是， 当worker 忙时， 不能自动扩容到2核或3核， 因此常常任务不能完成； 当worker不忙时， cpu却又不能共享出来；而真实运行中，发现这种方式的集群，cpu利用率非常低下， 一台机器，只有极少的worker 能使用满绑定的核， 绝大部分worker 处在空闲状态。
* 共享方式， 所有worker 申请一定的权重， 假设3个worker， worker a 权重100， 处于cpu 疯跑状态， worker b 权重 200， 处于疯跑， worker c 权重100 正常状态，最终结果就是 worker a 差不多用掉1/3 的cpu， worker b 差不多用掉 2/3 的cpu， worker c 基本用不了多少cpu， 但整个机器load会非常重, 不断报警.

jstorm 使用的方式 是共享方式 ＋  上限方式， 就是worker 按照权重申请cpu，但同时设置一个worker的上限cpu阀值（默认4核）， 也就是在共享的基础上，每个worker 使用的cpu 上限不能超过4个核， 这样既能保障机器worker 忙时，可以自动用掉2核或3核， 闲的时候，cpu能空闲给其他worker使用，更为关键是， 不会让一个worker 疯跑，不会因为一个worker疯跑，导致整个机器load 非常高，其他的worker基本抢不到cpu。

我们这种方式，在实际使用中， 效果非常好。 

补充一下：cgroups是control groups的缩写，是Linux内核提供的一种可以限制, 记录, 隔离进程组(process groups)所使用的物理资源(如：cpu,memory,IO 等等)的机制。


# 配置

在Jstorm中，我们使用cgroup进行cpu硬件资源的管理。使用前，需要做如下检查和配置。

*  检查/etc/passwd 文件中当前用户的uid和gid， 假设当前用户是admin， 则看/etc/passwd文件中admin的uid和gid是多少
*  cgroup功能在当前系统的内核版本是否支持

检查/etc/cgconfig.conf是否存在。如果不存在， 请“yum install libcgroup”，如果存在，设置cpu子系统的挂载目录位置，
以及修改该配置文件中相应的uid/gid为启动jstorm用户的uid/gid， 本例子中以500为例， 注意是根据第一步来进行设置的。
  
```
  mount {    
      cpu = /cgroup/cpu;
  }
  

  group jstorm {
       perm {
               task {
                      uid = 500;
                      gid = 500;
               }
               admin {
                      uid = 500;
                      gid = 500;
               }
       }
       cpu {
       }
  }
```

这是一个cgconfig.conf配置文件例子。比如jstorm的启动用户为admin，admin在当前
  系统的uid/gid为500（查看/etc/passwd 可以查看到uid和gid），那么相对应cpu子系统的jstorm目录uid/gid也需要设置为相同的值。
  以便jstorm有相应权限可以在这个目录下为jstorm的每个需要进行资源隔离的进程创建对应
  的目录和进行相关设置。



### 启动cgroup服务

```
service cgconfig restart
chkconfig --level 23456 cgconfig on
```

  Note: cgconfig.conf只能在root模式下修改。

### 或者直接执行命令

```
mkdir /cgroup/cpu
mount  -t cgroup -o cpu none /cgroup/cpu
mkdir /cgroup/cpu/jstorm
chown admin:admin /cgroup/cpu/jstorm
```


## 在jstorm配置文件中打开cgroup, 配置storm.yaml

```
   supervisor.enable.cgroup: true
```

