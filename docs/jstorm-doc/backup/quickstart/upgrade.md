---
title: Upgrade Guide
layout: plain
top-nav-title: Upgrade Guide
top-nav-group: quickstart
top-nav-pos: 7
sub-nav-title: Upgrade Guide
sub-nav-group: quickstart
sub-nav-pos: 7
---
Currently, JStorm has divided into two series of releases: 0.9.x and 2.x. The 0.9.x series are basically backward compatible(after JStorm 0.9.6.2), and the 2.x series are basically backward compatible too. But note that 0.9.x and 2.x are NOT compatible.

The upgrade of two compatible versions are as follows:

* Stop all nimbus nodes & supervisors in your cluster


* Download release package of the new version and replace JStorm package to the original installation directory. Note you may need to delete jstorm-client/server/client-extension and libs first since different versions of jstorm-client/server jars may have different file names, which CANNOT reside within the same directory.

* Start nimbus and supervisors and check if the logs are normal.

* Restart all your topologies, note this step is not necessarily required but it's recommended to do so.

If you decide to upgrade 0.9.x to 2.x to experience some new features(e.g., powerful metrics, brand-new cool web UI), you need the following steps:


* Kill all your topologies

* Upgrade JStorm, which is almost the same as mentioned above, except you're using a 2.x version.

* Change JStorm dependencies of your topology project, this requires you to delete all old dependencies like "jstorm-client", "jstorm-server", etc., and add new dependency:

```
<dependency>
    <groupId>com.alibaba.jstorm</groupId>
    <artifactId>jstorm-core</artifactId>
    <version>2.1.1</version>
</dependency>
```

* Rebuild your topology project, if everything is OK, re-submit your topology.

Here's also a detailed doc about how to migrate JStorm 0.9.x to 2.x: https://github.com/alibaba/jstorm/wiki/Migration-Guide-From-JStorm0.9.x-To-2.x_en