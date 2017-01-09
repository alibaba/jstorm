---
title: WebUI 安装部署
is_beta: false

sub-nav-group: Deploy_cn
sub-nav-id: WebUI_cn
sub-nav-pos: 3
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}


# 概述
WebUI 的安装部署， 和JStorm 是完全独立的。而且，并不要求WebUI的机器必须是在jstorm机器中。

自JStorm 0.9.6.1 后， 一个web UI 可以管理多个集群， 只需在web ui的配置文件中，增加新集群的配置即可。

随着集群的规模不断增大， 集群的数量也越来越多， 尤其是一些小集群也逐渐增多， 如果继续一个集群一个web ui， PE的工作量自然非常巨大， 另外， 对于一些线上应用， 每搭建一个web ui， 需要层层审批， 比如，VIP 申请，开放端口申请， 域名申请， 每一个环节都十分痛苦， 因此，强烈需要一个web ui管理多个storm集群

----------

## 如何安装Web UI 

必须使用tomcat 7.x版本， 注意不要忘记拷贝 **~/.jstorm/storm.yaml**

Web UI 可以和Nimbus不在同一个节点

```
mkdir ~/.jstorm
cp -f $JSTORM_HOME/conf/storm.yaml ~/.jstorm
下载tomcat 7.x （以apache-tomcat-7.0.37 为例）
tar -xzf apache-tomcat-7.0.37.tar.gz
cd apache-tomcat-7.0.37
cd webapps
cp $JSTORM_HOME/jstorm-ui-2.1.1.war ./
mv ROOT ROOT.old
ln -s jstorm-ui-2.1.1 ROOT  #这个地方可能变化，是根据你的JStorm版本来确定，比如当0.9.6.1时，是ln -s jstorm-0.9.6.1 ROOT
                              另外不是 ln -s jstorm-ui-0.9.6.3.war ROOT 这个要小心
cd ../bin
./startup.sh
```

## WebUI 增加新集群
注意： web ui使用的版本必须和集群中JStorm最高的版本一致

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
