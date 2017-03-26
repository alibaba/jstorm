---
title: User Define Scheduler
layout: plain
top-nav-title: User Define Scheduler
top-nav-group: api
top-nav-pos: 3
---
From JStorm 0.9.0,JStorm provides a very powerful scheduling features, meet most needs Basically.
Before learning how to use the new scheduling,Please to learn what features provided by the introduction of  [JStorm 0.9.0](http://wenku.baidu.com/view/59e81017dd36a32d7375818b.html)

## Interface
## Set the default memory size for each worker
         ConfigExtension.setMemSizePerWorker(Map conf, long memSize)
modify the configurations, for example:

```
worker.memory.size: 2147483648
```
## Set weights for each worker's cgroup, cpu 
         ConfigExtension.setCpuSlotNumPerWorker(Map conf, int slotNum)
modify the configurations, for example:

```
worker.cpu.slot.num: 3
```
## Set whether to use the old distribution 
         ConfigExtension.setUseOldAssignment(Map conf, boolean useOld)
modify the configurations, for example:

```
use.old.assignment: true
```
## Set to force one component of the task to work on different nodes
         ConfigExtension.setTaskOnDifferentNode(Map conf, boolean isIsolate)
modify the configurations, for example:

```
task.on.differ.node: true
```
## Set to force topology running on special nodes
         conf.put(Config.ISOLATION_SCHEDULER_MACHINES, List<String> isolationHosts);
modify the configurations, for example:

```
isolation.scheduler.machines: 
    - machine1.com
    - machine2.com
    - machine3.com
```
## Custom worker assigned
   
   
         WorkerAssignment worker = new WorkerAssignment();
         worker.addComponent(String compenentName, Integer num);      //Add a task on this worker
         worker.setHostName(String hostName);      //force this worker on one machine 
         worker.setJvm(String jvm);                //this can be skipped, set jvm parameters of this worker 
         worker.setMem(long mem);                  //this can be skipped, set the worker's memory size
         worker.setCpu(int slotNum);               //this can be skipped, set the cpu's weights
         ConfigExtension.setUserDefineAssignment(Map conf, List<WorkerAssignment> userDefines)
PS:Each worker's argument does not need to be all set, Under the premise that worker attributes in the legal premise, even if only to set up some parameters will still take effect.

modify the configurations, for example:

```
use.userdefine.assignment: 
    - {"port":null,"nodeId":null,"componentToNum":"{\"componentname\":2}","mem":2147483648,"cpu":null,"hostName":"host1.com","jvm":null}
    - {"port":null,"nodeId":null,"componentToNum":"{\"componetname2\":2}","mem":null,"cpu":null,"hostName":null,"jvm":null}
```
## Default Scheduling Algorithm
*          Worker will be equally assign to every supervisors.
*          Task will be equally assign to every worker exception user defined workers.
*          Avoid assign the same kind of tasks on one worker as much as possible. Try best to assign sender/recever peer task in one worker.
*          New topology won't effect the old topology's assignment
*          User defined assignment has the highest priority, then the Old assignment if enable, at last the default assignment.  

