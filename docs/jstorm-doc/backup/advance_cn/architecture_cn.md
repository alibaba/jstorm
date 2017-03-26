---
title: JStorm架构
layout: plain_cn
top-nav-title: JStorm架构
top-nav-group: 进阶
top-nav-pos: 1
sub-nav-title: JStorm架构
sub-nav-group: 进阶
sub-nav-pos: 1
---
JStorm 从设计的角度，就是一个典型的调度系统。

在这个系统中， 

* Nimbus是作为调度器角色
* Supervisor 作为worker的代理角色，负责杀死worker和运行worker
* Worker是task的容器
* Task是真正任务的执行者
* ZK 是整个系统中的协调者

具体参考下图：

![jiagou]({{site.baseurl}}/img/advance_cn/architecture/architecture.jpg)