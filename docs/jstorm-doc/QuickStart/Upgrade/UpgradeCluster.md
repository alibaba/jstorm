---
title: Upgrade Cluster
is_beta: false

sub-nav-group: Upgrade
sub-nav-id: UpgradeCluster
sub-nav-pos: 3
---

* This will be replaced by the TOC
{:toc}

# Version Compatibility

This document illustrates the compatibilities between different JStorm version.
Note that versions earlier than JStorm0.9.8 will not be listed since they're pretty old. 


```
0.9.8.x ==> 2.1.0,  application code needs to re-compile and restart

2.1.0 ==> 2.1.1, application code doesn't need to re-compile, but needs to restart after upgrade
2.1.1 ==> 2.2.1, application code needs to re-compile and restart
  
```


# How to upgrade cluster

* Replace JStorm binaries, note that DO NOT replace jstorm-data and conf files.
* Restart JStorm cluster including nimbus and supervisor.
* Replace web-ui binary
* Restart web－ui 