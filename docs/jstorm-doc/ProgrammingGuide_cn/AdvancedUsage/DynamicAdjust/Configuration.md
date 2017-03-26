---
title: "Dynamic Adjust application configuration"
layout: plain_cn

# Sub navigation
sub-nav-parent: DynamicAdjust_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Configuration_cn
#sub-nav-pos: 2
sub-nav-title: 动态更新配置
---

* This will be replaced by the TOC
{:toc}

## Jstorm 支持动态更新配置文件
从Release 0.9.8.1 开始Jstorm支持动态修改拓扑的配置文件，更新后的配置文件能直接被应用响应，在响应过程中 topology任务不会被重启或者下线，保证服务的持续性。
     
Note：拓扑配置文件的更新不包括对worker和component的并发度调整，如果用户想动态调整应用worker和组件并发度的大小请参看[JStorm 任务的动态伸缩](JStorm-Worker-Task-Scale-out-in)

---

## 拓扑要求
拓扑中所有的component中只有继承了接口

```
public interface IDynamicComponent extends Serializable {
   public void update(Map conf);   //conf 配置文件
}
```

才能实现配置文件的动态更新，至于配置文件怎么去更新相应component的配置，是由用户事先实现好的接口update（）决定的。当用户提交动态更新配置文件的命令后，该函数会被回调。更新配置文件是以component为级别的，每个component都有自己的update，如果哪个component不需要实现配置文件动态更新，那它就无需继续该接口。

Note：需要实现动态更新配置文件的bolt，一定不要去继承接口IBasicBolt；

## 动态更新配置文件的使用

#### 1. 命令行方式

```
   USAGE: jstorm  update_topology  TopologyName  -conf configPath
   e.g. jstorm  update_topology  SequenceTest –conf  conf.yaml
   参数说明:
   TopologyName: Topology任务的名字
   configPath： 配置文件名称（暂时只支持yaml格式）
            
```

配置文件例子

```
topology.max.spout.pending: 10000
send.sleep.second: 100
kryo.enable: false
fall.back.on.java.serialization: true
```

#### 2. API接口
```
   backtype.storm.command.update_topology:
   -     private static void updateTopology(String topologyName, String pathJar,
            String pathConf)
       Note: 参数pathJar是用设置来实现jar的更新，没有的话可以设置为null
```

## 常见问题

#### bolt的update函数没有被回调

可能你这个Bolt继承了接口IBasicBolt，为了能够实现配置文件动态更新我们建议实现bolt继承接口IRichBolt和IDynamicComponent 

## 其他
其实jstorm还支持对jar的动态更新，这里只是对集群上jar的更新，无法做到jar升级后，应用动态升级。如果要实现应用的动态升级，需要借助Pandora模块化开发，具体可以参考[基于模块化开发可以实现应用动态升级的实例](http://gitlab.alibaba-inc.com/aloha/aloha-utility/tree/master/pandora_framework)

