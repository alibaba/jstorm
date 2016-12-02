---
title: 资源分组
layout: plain_cn
top-nav-title: 资源分组
top-nav-group: 进阶
top-nav-pos: 4
sub-nav-title: 资源分组
sub-nav-group: 进阶
sub-nav-pos: 4
---
JStorm 自0.95之后已经不支持将一个集群的资源进行分组隔离，原因是现在用hadoop-yarn就能够轻易地实现该需求，不同的部门可以在Yarn上启动互相独立的JStorm集群，且每个集群的资源都是相互独立的，这样规模较小的JStorm集群可以很容易的升级和维护。