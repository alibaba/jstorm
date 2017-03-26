---
title: "动态调整，启用/禁用metrics"
layout: plain_cn

# Sub navigation
sub-nav-parent: DynamicAdjust_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Metrics_cn
#sub-nav-pos: 1
sub-nav-title: 动态调整Metrics
---

* This will be replaced by the TOC
{:toc}

# JStorm2.x动态启用/禁用metrics

有时一个拓扑中metrics可能过多，用户并不需要看这么多的metrics，JStorm 2.1.1开始支持动态启用/禁用metrcs。

主要由这两个变量控制：`topology.enabled.metric.names`，`topology.disabled.metric.names`。

其中：

### topology.enabled.metric.names
需要动态enable的metrics名称列表，逗号分隔。这里的metrics，可能是你之前禁用掉的。

### topology.disabled.metric.names
需要动态disable的metrics名称列表，逗号分隔。

注意这两个配置的生效顺序是：`topology.enabled.metric.names` > `topology.disabled.metric.names`。
即，如果一个metric name即在enable的列表中，又是disable的列表中，那么只会对enable列表生效。

具体使用方法：

在koala中，找到你的topology，然后在topology页面，点击“重启拓扑”，再弹出的框中点“修改配置并重启”，并将你的配置写入到一个yaml中，上传重启即可。