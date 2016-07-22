---
title:  "How to enable supervisor health check?"
layout: plain_cn

# Top-level navigation
top-nav-group: Maintenance_cn
top-nav-pos: 4
top-nav-id: HealthCheck_cn
top-nav-title: Supervisor自检
---

* This will be replaced by the TOC
{:toc}

# 概述

Jstorm支持对集群进行健康检查，通过定时执行检测脚本获取机器的的健康状态，然后动态去调整集群。换句话说，jstorm可以根据机器的健康状态，会让supervisor主动触发执行动作，合理调整自身的状态。目前我们将superviosr的机器健康状态归类为4种情况：panic  error warn info。

```
   panic状态： 该状态下我们会首先将该机器上的所有worker杀死，最后supervisor进行自杀，让该机器从集群中移除；
   error状态： 该状态下会将该机器上的所有worker杀死，并将其的可用端口数量设置为0，让该机器不再参与集群的调度；
   warn状态：  该状态下会将其的可用端口数量设置为0，让该机器不再参与集群的调度；
   info状态：  健康状态，不做任何处理
  
```
Note: 这个文档暂时是针对于2.x版本的


# 配置
对于机器的健康状态检测是通过一些检测脚本获取的。这些脚本你可以根据你的要求自己去实现。我们设定每种健康状态对应一个脚本执行目录，该执行脚本目录是可配置的。
  
```
   panic脚本执行目录： 绝对路径，配置脚本目录参数storm.machine.resource.panic.check.dir
   error脚本执行目录： 绝对路径，配置脚本目录参数storm.machine.resource.error.check.dir
   warn脚本执行目录：  绝对路径，配置脚本目录参数storm.machine.resource.warning.check.dir
  
```

每个目录下健康检查脚本的数量是不受限制的。任何一个脚本检查到机器异常，该健康状态会立马被supervisor捕获。例如panic下的某个脚本检查到该机器异常，则supervisor捕获的的机器状态是panic；同样的warn目录下的任何一个脚本检查到机器异常，则supervisor捕获到的机器状态是warn。

## 对脚本的一点小小要求

由于jstorm里头做了限制，supervisor会根据执行脚本的输出来判断该机器是否异常。如果脚本输出是check don't passed, 则判断该执行脚本检查到该机器异常；其他情况下一律判断该机器状态是健康的。例如在warn目录下的检查cpu的脚本：

```
#!/usr/bin/env bash
MAX_cpu=70
top_command=`which top`
cpuInfo=`$top_command -b -n 1 | grep "Cpu(s)" | awk '{print $2+$3}'`
Cpu=${cpuInfo/.*}
if [ $Cpu -gt $MAX_cpu ];then
        echo "check don't passed"
fi

```
该机器的cpu利用率如果大于70%，则输出check don't passed,这时判断该机器状态处于warn状态。其他输出或者执行脚本异常或脚本执行超时统统会判断该supervisor处于info状态。

## **其他配置参数**

```
  supervisor.enable.check： 健康检查开发，默认关闭， 是supervisor级别的开关；
  supervisor.frequency.check.secs： 健康检测脚本的执行频率，默认是60s；
  storm.health.check.timeout.ms： 脚本执行超时时间，默认5s
```

