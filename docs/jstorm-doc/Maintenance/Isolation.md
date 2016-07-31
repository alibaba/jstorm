---
title:  "How to enable resource isolation?"
# Top-level navigation
top-nav-group: Maintenance
top-nav-pos: 3
top-nav-title: Resource Isolation
---

* This will be replaced by the TOC
{:toc}

# Overview
Resource isolation is the ability that every computing platform needs. The resource isolation are divided into several levels:

* Between clusters
- * Standalone, deploy the cluster in directly physical isolation. It has the best resource isolation effect. The disadvantage is easy to waste resources.
- * Jstorm-on-yarn, Jstorm-on-docker, through yarn or docker-swam, deploy multiple logical clusters on a physical cluster, and the resource is isolated between logic clusters.

* Inside a cluster
- * Custom scheduling, the user can force some tasks to run on certain machines and occupy them by jstorm customized scheduling.
- * Worker-cgroup, set the cgroup on the worker, isolate the cpu and memory resource.

This article mainly describes the worker-cgroup configuration inside the jstorm cluster.

Additionally: the 2 cgroup ways that industry commonly use, is not efficiency as the jstorm's way.

* Tied to cpu core, such as a commonly used way on yarn, a worker is bound to a cpu core. The biggest problem is that in this way, 
when the worker is busy, it can not expanded to 2 or 3 cores automatically. 
It often makes the task can not complete. When the worker is not busy, the cpu core can not be shared. 
In the true running environment, we found that the cpu occupation is very low in the cluster in this way.
Only a few worker can fully use the core which it binds with, most of the workers are in an idle status.

* Shared, all workers apply for a certain weight, assuming that there are three workers. Worker A weight of 100, in the cpu running wild status. Worker B weight 200, is in the cpu running wild status. Worker C weight of 100 and in normal status. The final result is that the worker A approximately used 1/3 of the cpu, worker B approximately used 2/3 of the cpu, worker C used little of the cpu. But the entire machine load will be very heavy, it alarms continuously.

Jstorm use a shared + limited way which is the workers apply cpu according to their weights, but at the same time set a threshold of worker's cpu (default 4-core). That is on a shared basis, limits the cpu of each worker used can not be more than four cores. It ensures when a worker is busy, it can automatically use 2 or 3 cores. In idle time, worker can free the cpu to let other workers use. Even more critical is that it will not let a worker run wildly. It will not lead to the machine load is very high because a worker runs wildly and the other workers could not use the cpu.

In the actual using, this way has a very good effect.

Additionally: cgroups is the abbreviation of control groups, which is a mechanism that provided by Linux kernel and can be used to restrict, record, isolate the physical resources used by process group(such as: cpu, memory, IO, etc.).

# Configuration

In Jstorm, we use cgroup manage the cpu hardware resources. Before use, check the following items and configuration.

* Check the current user's uid and gid in the file /etc/passwd. Assuming that the current user is admin, then check waht the admin uid and gid is in the file /etc/passwd.

* Whether the Cgroup function is supported or not in current system kernel version.

*Check if /etc/cgconfig.conf exists. If not, please "yum install libcgroup". If it exists, set cpu subsystem mount directory location. And modify the corresponding uid/gid in the configuration file to the uid/gid of the user that start jstorm. In this case 500, for example. Note that it is set according to the first step.

```
  mount {    
      cpu = /cgroup/cpu;
  }
  

  group jstorm {
       perm {
               task {
                      uid = 500;
                      gid = 500;
               }
               admin {
                      uid = 500;
                      gid = 500;
               }
       }
       cpu {
       }
  }
```

This is a cgconfig.conf profile examples. For example the jstorm start user is admin. The uid/gid of admin in the current System is 500 (see /etc/passwd can view the uid and gid), then jstorm directory uid/gid also needs to be set to the same value in the corresponding cpu subsystem. So jstorm have the appropriate permissions that can create the corresponding directory and make related settings in this directory for each jstorm process that needs to be isolated of resources.

### Start cgroup service

```
service cgconfig restart
chkconfig --level 23456 cgconfig on
```

  Note: cgconfig.conf can only be modified in root mode.


### Or directly Run Command

```
mkdir /cgroup/cpu
mount  -t cgroup -o cpu none /cgroup/cpu
mkdir /cgroup/cpu/jstorm
chown admin:admin /cgroup/cpu/jstorm
```


## Open cgroup in jstorm configuration file, configure storm.yaml

```
   supervisor.enable.cgroup: true
```
