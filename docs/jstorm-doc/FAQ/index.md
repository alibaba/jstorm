---
title: FAQ
layout: plain
---
## Performance issues
Reference Performance Optimization

## Lack of resources
When the report "No supervisor resource is enough for component", it means that the resource is not enough, if only the test environment can be supervisor of cpu and memory slot set bigger.

In jstorm in a task default consume a cpu slot and a memory slot, and a machine default  cpu slot number is cpu core -1, memory slot number is the physical memory size * 75% / 1g, if a run on worker task more, you need to set smaller memory slot size (default is 1G), such as 512M, memory.slot.per.size: 536870912 bytes.

```
 #if it is null, then it will be detect by system
 supervisor.cpu.slot.num: null

 #if it is null, then it will be detect by system
 supervisor.mem.slot.num: null

# support disk slot
# if it is null, it will use $(storm.local.dir)/worker_shared_data
 supervisor.disk.slot: null
```

##  Serialization issues
All spout, bolt, configuration, message (Tuple) sent must implement Serializable, otherwise there will be a  error of serialization.

When the spout or bolt if it is a member variable does not implement serializable, but when you have to use, you can increase the "transient" modifier when declaring variables, and  instantiated when you open or prepare .

## Log4j conflict
From 0.9.0, JStorm still use Log4J, but the storm using Logbak, so the application if there are dependent log4j-over-slf4j.jar, you need to exclude all log4j-over-slf4j.jar dependence, the next version will have a custom classloader, do not worry about this problem.

```
SLF4J: Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError. 
SLF4J: See also 
http://www.slf4j.org/codes.html#log4jDelegationLoop for more details.
Exception in thread "main" java.lang.ExceptionInInitializerError
        at org.apache.log4j.Logger.getLogger(Logger.java:39)
        at org.apache.log4j.Logger.getLogger(Logger.java:43)
        at com.alibaba.jstorm.daemon.worker.Worker.<clinit>(Worker.java:32)
Caused by: java.lang.IllegalStateException: Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError. See also 
http://www.slf4j.org/codes.html#log4jDelegationLoop for more details.
        at org.apache.log4j.Log4jLoggerFactory.<clinit>(Log4jLoggerFactory.java:49)
        ... 3 more
Could not find the main class: com.alibaba.jstorm.daemon.worker.Worker.  Program will exit.
```

## Class conflict
If the application and JStorm use the same jar package, but not the same version, it is recommended to modify the configuration file, open classloader

```
topology.enable.classloader: true
```
Or

```
ConfigExtension.setEnableTopologyClassLoader(conf, true);
```

JStorm default is to turn off the classloader, therefore JStorm will be forced to use JStorm dependent jar

## After submitting the task, and after waiting a few minutes, web ui has not display the corresponding task
three kinds of situations:

### User application initialization is too slow
If the user application has log output, it indicates that the initialization of the application is too slow or error, you can view the log. In addition to MetaQ 1.x applications, Spout will recover ~ /.meta_recover/ directory files, you can delete these files, acceleration starts.

### Usually the user jar conflict or a problem with the initialization
Open supervisor logs, to identify start worker command, executed individually, and then check if there are problems. Similar to the following:

![fail_start_worker]({{site.baseurl}}/img/FAQ/fail_start_worker.jpg)

### Check is not the same storm and jstorm local directory
Check the configuration items "storm.local.dir", storm and jstorm whether to use the same local directory,  if the same, to a different directory

## port has been bound
two kinds of situations:

### More than one worker to seize a port
Assuming 6800 port is occupied, you can execute the command "ps -ef | grep 6800" to check whether there are multiple processes, if there are multiple processes, kill them manually

### open too many connections
Linux exists  the external connection port limit, TCP client initiates a connection outside reached about 28,000, began to throw a lot of exceptions, you need to modify the external connection port restrictions
```
 # echo "10000 65535" > /proc/sys/net/ipv4/ip_local_port_range
```


Other questions, You can enter the QQ group (228374502) for consultation