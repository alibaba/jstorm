---
title: Standalone模式安装
is_beta: false

sub-nav-group: Deploy_cn
sub-nav-id: Standalone_cn
sub-nav-pos: 2
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}

# 安装步骤

* 从[下载页面]({{site.baseurl}}/downloads/)下载relase包
* 搭建外部依赖
  * 搭建Zookeeper集群
  * 安装Java 
* 安装jstorm
* 启动JStorm集群

# 搭建外部依赖

## 搭建Zookeeper集群

如果公司有现成zookeeper，则直接使用，无须安装。

本处不细描叙Zookeeper安装步骤

* 安装步骤麻烦参考 [”Zookeeper 安装步骤“](http://www.cnblogs.com/linjiqin/archive/2013/03/16/2962597.html)
* Zookeeper配置麻烦参考 [“Zookeeper 配置介绍”](http://blog.csdn.net/hi_kevin/article/details/7089358)

## 安装JDK 7
如果机器上已经安装了jdk7, 则无需再安装jdk7，

注意：
* 如果当前系统是64位系统，则需要下载64位JDK，如果是32为系统，则下载32位JDK
* jstorm 2.x 版本开始，要求jdk版本必须等于或高于jdk7



# 搭建JStorm集群

### 检查
* 检查机器ip是否正确
执行`hostname -i` 如果返回“127.0.0.1”,  则机器没有配置正确的ip， 需要设定/etc/hosts或网卡配置， 直到`hostname -i`返回一个正确的ip

* 检查 java 版本
执行`java -version`, 如果找不到java 或java 版本低于7， 则需要设置PATH环境变量或安装jdk7

### 安装JStorm

假设以jstorm-2.1.1.zip为例

```
unzip jstorm-2.1.1.zip
vi ~/.bashrc
export JSTORM_HOME=/XXXXX/XXXX
export PATH=$PATH:$JSTORM_HOME/bin
```

### 配置$JSTORM_HOME/conf/storm.yaml

基本配置项：

* storm.zookeeper.servers: 表示zookeeper 的地址，
* storm.zookeeper.root: 表示JStorm在zookeeper中的根目录，**当多个JStorm共享一个zookeeper时，需要设置该选项**，默认即为“/jstorm”
* nimbus.host: 表示nimbus的地址， 填写ip
* storm.local.dir: 表示JStorm临时数据存放目录，需要保证JStorm程序对该目录有写权限

其他详细配置，请参考[配置详解]({{site.baseurl}}/Maintenance/Configuration.html)

### 部署其他节点时
请确保其他节点的‘临时数据存放’目录为空， ‘临时数据存放’ 为$JSTORM_HOME/conf/storm.yaml 中指定storm.local.dir的目录

### 启动JStorm

* 在nimbus 节点上执行 “nohup jstorm nimbus &”, 查看$JSTORM_HOME/logs/nimbus.log检查有无错误
* 在supervisor节点上执行 “nohup jstorm supervisor &”, 查看$JSTORM_HOME/logs/supervisor.log检查有无错误


