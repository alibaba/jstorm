---
title: "User Defined Scheduler, Assign worker whatever user want"
layout: plain_cn

# Sub navigation
sub-nav-parent: UserDefined_cn
sub-nav-group: AdvancedUsage_cn
sub-nav-id: Scheduler_cn
#sub-nav-pos: 2
sub-nav-title: 自定义调度
---

* This will be replaced by the TOC
{:toc}

从JStorm 0.9.0 开始， JStorm 提供非常强大的调度功能， 基本上可以满足大部分的需求。

在学习如何使用新调度前， 麻烦先学习 [JStorm 0.9.0介绍](http://wenku.baidu.com/view/59e81017dd36a32d7375818b.html) 提供哪些功能

## 接口

### 设置每个worker的默认内存大小
```
       ConfigExtension.setMemSizePerWorker(Map conf, long memSize)
```

### 设置每个worker的cgroup,cpu权重
```
       ConfigExtension.setCpuSlotNumPerWorker(Map conf, int slotNum)
```

### 设置是否使用旧的分配方式
```
       ConfigExtension.setUseOldAssignment(Map conf, boolean useOld)
```

### 设置强制某个component的task 运行在不同的节点上
```
       ConfigExtension.setTaskOnDifferentNode(Map componentConf, boolean isIsolate)
```

注意，这个配置componentConf是component的配置， 需要执行addConfigurations 加入到spout或bolt的configuration当中

### 自定义worker分配
```
      WorkerAssignment worker = new WorkerAssignment();
      worker.addComponent(String compenentName, Integer num);//在这个worker上增加一个task
      worker.setHostName(String hostName);//强制这个worker在某台机器上
      worker.setJvm(String jvm);//设置这个worker的jvm参数
      worker.setMem(long mem); //设置这个worker的内存大小
      worker.setCpu(int slotNum); //设置cpu的权重大小
      ConfigExtension.setUserDefineAssignment(Map conf, List<WorkerAssignment> userDefines)
```
注:每一个worker的参数并不需要被全部设置,worker属性在合法的前提下即使只设置了部分参数也仍会生效

### 强制topology运行在一些supervisor上
在实际应用中， 常常一些机器部署了本地服务（比如本地DB）， 为了提高性能， 让这个topology的所有task强制运行在这些机器上
```
conf.put(Config.ISOLATION_SCHEDULER_MACHINES, List<String> isolationHosts);
```
conf 是topology的configuration

## 调度细则
- 任务调度算法以worker为维度
- 调度过程中正在进行的调度动作不会对已发生的调度动作产生影响
- 调度过程中用户可以自定义useDefined Assignment，和使用已有的old Assignment，这两者的优先级是：useDefined Assignment>old Assignment
- 用户可以设置task.on.differ.node参数，强制要求同组件的task分布到不同supervisor上

## 默认调度算法
- 以worker为维度，尽量将worker平均分配到各个supervisor上
- 以worker为单位，确认worker与task数目大致的对应关系(注意在这之前已经其他拓扑占用利用的worker不再参与本次动作)
- 建立task-worker关系的优先级依次为：尽量避免同类task在同一work和supervisor下的情况，尽量保证task在worker和supervisor基准上平均分配，尽量保证有直接信息流传输的task在同一worker下
