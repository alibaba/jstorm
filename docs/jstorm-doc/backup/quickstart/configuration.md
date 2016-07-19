---
title: JStorm Configuration
layout: plain
top-nav-title: JStorm Configuration
top-nav-group: quickstart
top-nav-pos: 4
sub-nav-title: JStorm Configuration
sub-nav-group: quickstart
sub-nav-pos: 4
---
The page doesn't list all setting. If you want to know all setting, please extract "defaults.yaml" from jstorm-server-xxxx.jar. Here just list frequently used setting.

```
storm.zookeeper.servers: zookeeper address.

storm.zookeeper.root: root directory of JStorm in zookeeper. When multiple JStorm system share a ZOOKEEPER, you need to set this option. the default is "/jstorm".

nimbus.host: nimbus ip, this setting is only for $JSTORM_HOME/bin/start.sh script.

storm.local.dir: JStorm temporary directory to store local binary or configuration. You need to make ensure JStorm program has written privilege to this directory. 

java.library.path: zeromq and java zeromq library installation directory, if you are using other shared library, please put them in this directory. The default is "/usr/local/lib:/opt/local/lib:/usr/lib".

supervisor.slots.ports: a list of ports provided by the supervisors. Be careful not to conflict with other ports. The default is 68xx, while storm is 67xx.

topology.enable.classloader: false, classloader is disabled by default. If the jar of the application is conflict with one of jares which JStorm depends on. For example, an application depends on thrift9, but JStorm uses thrift7, then you need to enable this configure item.

## send message with sync or async mode
## if this setting is true, netty will use sync mode which means client can send one batch message only after receive one server's response
## Async mode means client can send message without server's response
storm.messaging.netty.sync.mode: false

## when netty is in async mode and client channel is unavailable( server is down or netty channel buffer is full), 
## it will block sending until channel is ready or channel is closed
storm.messaging.netty.async.block: true

#This setting is useless when netty is in sync mode.
# If this setting is true and netty is in async mode, netty will batch message
# if this setting is false and netty is in async mode, netty will send tuple one by one without batch tuple into one big message.
storm.messaging.netty.transfer.async.batch: true

### default worker memory size, unit is byte
 worker.memory.size: 2147483648

# Metrics Monitor
# topology.performance.metrics: it is the switch flag for performance 
# purpose. When it is disabled, the data of timer and histogram metrics 
# will not be collected.
 topology.performance.metrics: true
# topology.alimonitor.metrics.post: If it is disable, metrics data
# will only be printed to log. If it is enabled, the metrics data will be
# posted to alimonitor besides printing to log.
 topology.alimonitor.metrics.post: false


# when supervisor is shutdown, automatically shutdown worker
# if run jstorm under other container such as hadoop-yarn, 
# this must be set as true
 worker.stop.without.supervisor: false

#set how many tuple can spout send in one time.
# For example, if this is setting 100, 
# spout can't send the No. 101th tuple until spout receive one tuple's ack message
 topology.max.spout.pending: null
```

***

# The default storm.yaml
```
########### These MUST be filled in for a storm configuration
 storm.zookeeper.servers:
     - "localhost"

 storm.zookeeper.root: "/jstorm"
 
# %JSTORM_HOME% is the jstorm home directory
 storm.local.dir: "%JSTORM_HOME%/data"
 
 java.library.path: "/usr/local/lib:/opt/local/lib:/usr/lib"


 supervisor.slots.ports:
    - 6800
    - 6801
    - 6802
    - 6803

# Default disable user-define classloader
# If there are jar conflict between jstorm and application, 
# please enable it 
 topology.enable.classloader: false

# enable supervisor use cgroup to make resource isolation
# Before enable it, you should make sure:
# 	1. Linux version (>= 2.6.18)
# 	2. Have installed cgroup (check the file's existence:/proc/cgroups)
#	3. You should start your supervisor on root
# You can get more about cgroup:
#   http://t.cn/8s7nexU
 supervisor.enable.cgroup: false


### Netty will send multiple messages in one batch  
### Setting true will improve throughput, but more latency
 storm.messaging.netty.transfer.async.batch: true
 
### default worker memory size, unit is byte
 worker.memory.size: 2147483648

# Metrics Monitor
# topology.performance.metrics: it is the switch flag for performance 
# purpose. When it is disabled, the data of timer and histogram metrics 
# will not be collected.
# topology.alimonitor.metrics.post: If it is disable, metrics data
# will only be printed to log. If it is enabled, the metrics data will be
# posted to alimonitor besides printing to log.
 topology.performance.metrics: true
 topology.alimonitor.metrics.post: false
```