---
title: 资源硬隔离
layout: plain_cn
top-nav-title: 资源硬隔离
top-nav-group: 快速开始
top-nav-pos: 11
sub-nav-title: 资源硬隔离
sub-nav-group: 快速开始
sub-nav-pos: 11
---
cgroups是control groups的缩写，是Linux内核提供的一种可以限制, 记录, 隔离进程组(process groups)所使用的物理资源(如：cpu,memory,IO 等等)的机制。

在Jstorm中，我们使用cgroup进行cpu硬件资源的管理。使用前，需要做如下检查和配置。

*  检查/etc/passwd 文件中当前用户的uid和gid， 假设当前用户是admin， 则看/etc/passwd文件中admin的uid和gid是多少
* cgroup功能在当前系统的内核版本是否支持

检查/etc/cgconfig.conf是否存在。如果不存在， 请“yum install libcgroup”，如果存在，设置cpu子系统的挂载目录位置，
以及修改该配置文件中相应的uid/gid为启动jstorm用户的uid/gid， 本例子中以500为例， 注意是根据第一步来进行设置的。
  
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

然后启动cgroup服务

```
service cgconfig restart
chkconfig --level 23456 cgconfig on
```

  Note: cgconfig.conf只能在root模式下修改。

### 或者直接执行命令

这是一个cgconfig.conf配置文件例子。比如jstorm的启动用户为admin，admin在当前
  系统的uid/gid为500（查看/etc/passwd 可以查看到uid和gid），那么相对应cpu子系统的jstorm目录uid/gid也需要设置为相同的值。
  以便jstorm有相应权限可以在这个目录下为jstorm的每个需要进行资源隔离的进程创建对应
  的目录和进行相关设置。

```
mkdir /cgroup/cpu
mount  -t cgroup -o cpu none /cgroup/cpu
mkdir /cgroup/cpu/jstorm
chown admin:admin /cgroup/cpu/jstorm
```


## 在jstorm配置文件中打开cgroup, 配置storm.yaml

```
   supervisor.enable.cgroup: true
```