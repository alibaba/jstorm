---
title: 集群升级
is_beta: false

sub-nav-group: Upgrade_cn
sub-nav-id: UpgradeCluster_cn
sub-nav-pos: 2
layout: plain_cn
---

* This will be replaced by the TOC
{:toc}

# 版本兼容性

本wiki主要描叙一些上下兼容版本之间的升级比如0.9.5.x 升级到0.9.6.x， 直接rpm upgrade（或者直接binary替换）即可，非兼容系列需要重启所有应用

```
0.9.6.x ~ 0.9.7.x(含) 从0.9.5.x/0.9.4.x 升级到 0.9.6.x或0.9.7.x，应用代码无需重新编译，但集群升级完后，需要重启应用         
0.9.8.x               从0.9.6.x或0.9.7.x 升级到 0.9.8.x，应用代码无需重新编译，但集群升级完后，需要重启应用

从0.9.8.x系列升级到2.x， 应用源码需要重新编译

2.1.0  ~ 2.1.1     从2.1.0 升级到2.1.1， 应用代码无需重新编译，但集群升级完后，需要重启应用
2.2.0  ~ 2.2.x    从2.1.x/2.1.0 升级到2.2.x， 应用代码需要重新编译
```


# 升级过程

* 替换jstorm引擎binary， 注意本地配置目录和配置文件不要被覆盖掉
* 重启jstorm
* 替换web-ui binary
* 重启web－ui 