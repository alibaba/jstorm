---
title: 升级指南
layout: plain_cn
top-nav-title: 升级指南
top-nav-group: 快速开始
top-nav-pos: 7
sub-nav-title: 升级指南
sub-nav-group: 快速开始
sub-nav-pos: 7
---
JStorm目前版本分为两个系列：0.9.x和2.x。
其中0.9.x系列是基本兼容的，2.x系列也基本兼容。但是，**0.9.x和2.x之间不兼容**。

兼容版本之间的升级，主要步骤如下，以0.9.x为例：

* 停掉整个集群的Nimbus和Supervisor
* 下载新版本的release包，替换到原来的安装目录。注意这一步有可能需要将${JSTORM_HOME}中的jstorm-client, jstorm-server, lib/等先删掉（建议在删之前先备份），不然${JSTORM_HOME}下同时存在多个版本的jstorm-client/server是会有问题的
* 重启整个集群的Nimbus和Supervisor，检查日志是否正常
* 重启集群所有topology。注意这一步并非必须，但是建议重启。

如果需要从0.9.x升级至2.x（比如为了体验2.x骚气好用又强大的web UI)，需要以下步骤：

* 升级jstorm安装包，同上面的升级步骤，只不过现在是用2.x的安装包覆盖原来0.9.x的安装包
* 重新编译打包你的topology代码：注意这里需要将原来的jstorm依赖都删除，然后添加新的依赖：

```
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.1.1</version>
</dependency>
```

* 编译后，如果没问题，杀死原来的topology，然后重新提交。

另外，关于从0.9.x迁移到2.x，见：[从JStorm 0.9.x迁移至2.x]({{site.baseurl}}/quickstart_cn/migrate_cn.html)