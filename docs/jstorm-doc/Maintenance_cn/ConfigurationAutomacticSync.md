---
title:  "How to enable supervisor automatically sync configuration from Nimbus."
layout: plain_cn


# Top-level navigation
top-nav-group: Maintenance_cn
top-nav-id: ConfigurationAutomacticSync_cn
top-nav-pos: 2
top-nav-title: 自动同步配置文件
---

* This will be replaced by the TOC
{:toc}

## 前提

jstorm版本高于或等于 2.2.0

## 背景

目前修改jstorm的集群配置(storm.yaml)，需要在人肉到跳板机上先复制storm.yaml，编辑，然后用批量scp复制至集群并重启整个集群。如果需要对多个集群同时修改，效率太低，代价太大。

全局配置推送主要针对对集群配置的批量修改这个功能而开发。

## 实现

### standalone
jstorm-core中nimbus添加ConfigUpdateHandler插件，允许动态修改storm.yaml配置。默认的DefaultConfigUpdateHandler什么也不做，相当于没有插件。

同时实现了一个DiamondConfigUpdateHandler（external/jstorm-configserver模块），这个插件通过diamond做配置更新。当检测到有更新时，会备份当前storm.yaml（最多保留3个备份，分别为storm.yaml.1, storm.yaml.2, storm.yaml.3），然后将最新的配置写入到storm.yaml中。

supervisor会有一个配置更新线程（SupervisorRefershConfig），每隔半分钟左右向nimbus请求当前最新的配置，同时与本地配置做比较。如果不相同，则备份并覆盖本地配置。以完成配置的集群级别动态更新。

总体上来说，配置更新是从diamond推至nimbus，然后supervisor从nimbus拉取。

### jstorm-on-yarn
在yarn的情况下，情况稍微有点特殊。因为yarn的supervisor，有的配置项是动态生成的，如storm.local.dir, jstorm.log.dir等。这时如果直接把nimbus的配置覆盖掉supervisor的配置，是有问题的。

因此，在yarn的情况下，新加入了一个配置项，其默认值如下：

```
jstorm.yarn.conf.blacklist: "jstorm.home,jstorm.log.dir,storm.local.dir,java.library.path"
```

这个配置项指示，在supervisor端，对这几个配置，不要用nimbus的覆盖本地的。

具体实现上，在SupervisorRefershConfig初始化的时候，就会将这几个配置项保存起来，称为retainedYarnConfig。
后续与nimbus做配置比较时，是在过滤了这几个配置项的前提下做比较，如果配置不一样，则用**nimbus的配置，并在最后面追加retainedYarnConfig**，生成新的配置文件，然后覆盖本地的storm.yaml。

同时，为了区分是否yarn环境，需要对yarn的supervisor在启动时加入 `-Djstorm-on-yarn=true`参数。

需要注意的是，yarn的配置项黑名单，目前只支持简单的k-v格式，如果配置值为list或者map这种复杂格式，是有问题的。


## 操作

1. 在管控平台上触发对特定集群/所有集群的配置更新

2. 管控平台上会先从nimbus拉取当前的配置，让用户在线编辑，编辑完后推送

3. 每个集群会在diamond中保存当前配置文件，dataId为配置文件中cluster.name，为了保证所有环境都能收到配置，koala会将这个配置推送到diamond的所有环境（中心+单元）。

4. nimbus收到配置，与本地做检查，如果不一样，则更新。

5. supervisor拉取nimbus配置，根据是否yarn，与本地配置做检查，然后确认要不要更新。


## TODO

目前在收到新的配置后，实际上会回调RefreshableComponent.refresh来动态更新配置。但是现在支持的动态更新配置有限，主要是metrics、log相关的配置。2.2.1版本中，需要支持更多的配置项的自动更新。
