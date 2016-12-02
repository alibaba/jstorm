---
title: 单Web UI 管理多集群
layout: plain_cn
top-nav-title: 单Web UI 管理多集群
top-nav-group: 快速开始
top-nav-pos: 9
sub-nav-title: 单Web UI 管理多集群
sub-nav-group: 快速开始
sub-nav-pos: 9
---
自JStorm 0.9.6.1 后， 一个web UI 可以管理多个集群， 只需在web ui的配置文件中，增加新集群的配置即可。


随着集群的规模不断增大， 集群的数量也越来越多， 尤其是一些小集群也逐渐增多， 如果继续一个集群一个web ui， PE的工作量自然非常巨大， 另外， 对于一些线上应用， 每搭建一个web ui， 需要层层审批， 比如，VIP 申请，开放端口申请， 域名申请， 每一个环节都十分痛苦， 因此，强烈需要一个web ui管理多个storm集群

----------

## 准备工作 

1. 已经安装了一套web ui， [如何安装Web UI](https://github.com/alibaba/jstorm/wiki/%E5%A6%82%E4%BD%95%E5%AE%89%E8%A3%85#%E5%AE%89%E8%A3%85jstorm-web-ui) 
2. web ui使用的版本必须和集群中JStorm最高的版本一致

## 增加新集群

1. 在运行Web UI的机器上， 修改~/.jstorm/storm.yaml
2. 把默认的ui.clusters注释给去掉， 补充

```
# UI MultiCluster
# Following is an example of multicluster UI configuration
 ui.clusters:
     - {
         name: "jstorm.share",
         zkRoot: "/jstorm",
         zkServers:
             [ "zk.test1.com", "zk.test2.com", "zk.test3.com"],
         zkPort: 2181,
       }
     - {
         name: "jstorm.bu1",
         zkRoot: "/jstorm.dw",
         zkServers:
             [ "10.125.100.101", "10.125.100.101", "10.125.100.101"],
         zkPort: 2181,
       }
```

解释一下

```
- {
         name: "jstorm.bu1",    --- 这个集群的名字， 每个集群的名字必须不一样
         zkRoot: "/jstorm.dw",  --- 这个集群 zk的根节点，可以参考$JSTORM_HOME/con/storm.yaml 中“storm.zookeeper.root” 字段
         zkServers:
             [ "10.125.100.101", "10.125.100.101", "10.125.100.101"],   -- zk 机器列表
         zkPort: 2181,                                                  -- zk 的客户端端口
  }
```
