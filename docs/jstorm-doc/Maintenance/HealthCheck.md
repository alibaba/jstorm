---
title:  "How to enable supervisor health check?"
# Top-level navigation
top-nav-group: Maintenance
top-nav-pos: 4
top-nav-title: Supervisor Health Check
---

* This will be replaced by the TOC
{:toc}

# Overview

Jstorm supports cluster health checks by running a detection script periodically to get the machine health status, and then adjust the cluster dynamically.In other words, jstorm can let the supervisor trigger the execution by itself according to the health status of the machine, rationally adjust their status. Currently the supervisor machine health status is classified as four cases: panic error warn and info.

```
   panic status: In this status, we will kill all the workers on the machine firstly, then let the supervisor suicide. That makes the machine is removed from the cluster;
   error status: In this status, we will kill all the workers on the machine and the number of available ports is set to 0. That makes the machine is no longer participating in the cluster scheduling;
   warn status: In this status, the number of available ports is set to 0. That makes the machine is no longer participating in the cluster scheduling;
   info status : healthy status, do nothing with it.
```

Note: This document is temporarily for the version 2.x

# Configure

The health checks to the machine is done through some detection scripts. You can implement the scripts according to your own requirements. We set each health status corresponds to a directory for script execution, the directory is configurable.

```
   panic script execution directory: absolute path, script directory configure parameter is storm.machine.resource.panic.check.dir
   error script execution directory: absolute path, script directory configure parameter is storm.machine.resource.error.check.dir
   warn script execution directory: absolute path, script directory configure parameter is storm.machine.resource.warning.check.dir 
```

The number of health check scripts under each directory is unlimited. The health status will be captured by supervisor immediately if any script detects something abnormal on a machine. For example, if a panic script detects the abnormality on the machine, the supervisor capture the panic status of the machine. Similarly, if a warn script detects the abnormality on the machine, the supervisor capture the warn status of the machine.

## Little requirements to the script

Since it have been restricted inside jstorm, supervisor determines whether the machine is abnormal base on the output of the script is executed. If the script output is "check do not passed", it is regarded as that the execution of the script detected the abnormality on the machine. In other cases, it will be regarded as that the status of the machine is healthy. For example, a script checking cpu under the warn directory:

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

If the occupation of the cpu is more than 70%, it outputs "check do not passed". Then it is regarded as that the machine is in warn status. Any other output or time out occurred during the execution will be regarded as the supervisor is in info status.

## ** Other configuration parameters **

```
  supervisor.enable.check: health check enable, off by default. It is the enable of supervisor level;
  supervisor.frequency.check.secs: the execution frequency of the health check, 60s by default;
  storm.health.check.timeout.ms: time out of the the execution, 5s by default;
```

