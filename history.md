[JStorm English introduction](http://42.121.19.155/jstorm/JStorm-introduce-en.pptx)
[JStorm Chinese introduction](http://42.121.19.155/jstorm/JStorm-introduce.pptx)

# Release 2.1.1

## New features
1. 1.5~6X performance boost from worst to best scenarios compared to JStorm-2.1.0
1. Add application-level auto-batch
1. Add independent control channel to separate control msgs from biz msgs to guarantee high priority for control msgs
1. Dramatic performance boost in metrics, see "Improvements" section
1. Support jdk1.8
1. Add Nimbus hook and topology hook
1. Metrics system:
    1. Support disable/enable metrics on the fly
    1. Add jstorm metrics design docs, see JSTORM-METRICS.md
1. JStorm web UI:
    1. Add zookeeper viewer in web UI, thanks to @dingjun84
    1. Add log search and deep log search, support both backward search and forward search
    1. Support log file download
1. Support changing log level on the fly
1. Change error structure in zk, add errorLevel, errorCode and duration.
1. Add supervisor health check
1. Add -Dexclude.jars option to enable filtering jars manually

## Improvements
1. Metrics:
    1. use JHistogram/JMeter instead of Histogram/Meter, change internal Clock.tick to System.currentTimeMillis to improve performance (50+% boost in Meter and 25%+ boost in Histogram)
    1. add TupleLifeCycle metric
    1. add supervisor metrics: total_cpu_usage, total_mem_usage, disk_usage
    1. remove some unnecessary metrics like emitTime, etc.
    1. Use HeapByteBuffer instead of List<Long> to transmit metric data points, reduce 60+% metrics memory usage 
    1. Change sample rate from 10% to 5% by default
    1. Remove AsmTimer and related code
1. Log related:
    1. Use logback by default instead of log4j, exclude slf4j-log4j12 dependency
    1. Use jstorm.log.dir property instead of ${jstorm.home}/logs, see jstorm.logback.xml 
    1. Change all log4j Logger's to slf4j Logger's
    1. Set default log page size(log.page.size) in defaults.yaml to 128KB (web UI)
    1. Change topology log structure, add ${topology.name} directory, see jstorm.logback.xml
    1. Add timestamp in supervisor/nimbus gc log files; backup worker gc log before launching a new worker;
    1. Set logback/log4j file encoding to UTF-8
1. Refine backpressure stragety to avoid over-backpressure
1. Change acker pending rotating map to single thread to improve performance
1. Update RefreshConnections to avoid downloading assignments from zk frequently
1. Change default memory of Supervisor to 1G (previous 512MB)
1. Use ProcessLauncher to launch processes
1. Add DefaultUncaughtExceptionHandler for supervisor and nimbus
1. Change local ports to be different from 0.9.x versions (supervisor.slots.ports.base, nimbus.thrift.port, 
 nimbus.deamon.logview.port, supervisor.deamon.logview.port)
1. Change highcharts to echarts to avoid potential license violation
1. Dependency upgrades:
    1. Upgrade kryo to 2.23.0
    1. Upgrade disruptor to 3.2.2

## Bug fix
1. Fix deadlock when starting workers
1. Fix the bug that when localstate file is empty, supervisor can't start
1. Fix kryo serialization for HeapByteBuffer in metrics
1. Fix total memory usage calculation
1. Fix the bug that empty worker is assigned when configured worker number is bigger than the actual number for user defined scheduler
1. Fix UI log home directory 
1. Fix XSS security bug in web UI
1. Don't start TopologyMetricsRunnable thread in local mode, thanks to @L-Donne
1. Fix JSTORM-141, JSTORM-188 that TopologyMetricsRunnable consumes too much CPU
1. Remove MaxTenuringThreshold JVM option support jdk1.8, thanks to @249550148
1. Fix possible NPE in MkLocalShuffer

## Deploy and scripts
1. Add cleanup for core dumps
1. Add supervisor health check in healthCheck.sh
1. Change jstorm.py to terminate the original python process when starting nimbus/supervisor

## Upgrade guide
1. JStorm 2.1.1 is mostly compatible with 2.1.0, but it's better to restart your topologies to finish the upgrade.
1. If you're using log4j, be cautious that we have switched default logging system to logback, if you still want to use log4j, please add "user.defined.log4j.conf: jstorm.log4j.properties" to your conf/storm.yaml.


# Release 2.1.0

## New features

1. Totally redesign Web UI
	1.	Make the UI more beautiful
	1.	Improve Web UI speed much.
	1.	Add Cluster/Topology Level Summarized Metrics in recent 30 minutes.
	1.	Add DAG in the Web UI, support Uer Interaction to get key information such as emit, tuple lifecycle, tps
1. Redesign Metrics/Monitor System
	1.	New metrics core, support sample with more metric, avoid noise, merge metrics automatically for user.
	1.	No metrics will be stored in ZK
	1.	Support metrics HA
	1.	Add more useful metrics, such as tuple lifecycle, netty metrics, disk space etc. accurately get worker memory
	1.	Support external storage plugin to store metrics.
1. Implement Smart BackPressure 
	1.	Smart Backpressure, the dataflow will be more stable, avoid noise to trigger
	1.	Easy to manual control Backpressure
1. Implement TopologyMaster
	1.	Redesign hearbeat mechanism, easily support 6000+ tasks
	1.	Collect all task's metrics, do merge job, release Nimbus pressure.
	1.	Central Control Coordinator, issue control command
1. Redesign ZK usage, one set of ZK support more 2000+ hardware nodes.
	1.	No dynamic data in ZK, such as heartbeat, metrics, monitor status.
	1.	Nimbus reduce visiting ZK frequence when serve thrift API.
	1.	Reduce visiting ZK frequence, merge some task level ZK node.
	1.	Reduce visiting ZK frequence, remove useless ZK node, such as empty taskerror node
	1.	Tuning ZK cache  
	1.  Optimize ZK reconnect mechanism
1. Tuning Executor Batch performance
	1.	Add smart batch size setting
	1.	Remove memory copy
	1.	Directly issue tuple without batch for internal channel
	1.	Set the default Serialize/Deserialize method as Kryo
1. Set the default Serialized/Deserialized method as Kryo  to improve performance.
1. Support dynamic reload binary/configuration
1. Tuning LocalShuffle performance, Set 3 level priority, local worker, local node, other node, add dynamic check queue status, connection status.
1. Optimize Nimbus HA, only the highest priority nimbuses can be promoted as master 

## Improvement
1. Supervisor automatically dump worker jstack/jmap, when worker's status is invalid.
1. Supervisor can generate more ports according to memory.
1. Supervisor can download binary more time.
1. Support set logdir in configuration
1. Add configuration "nimbus.host.start.supervisor"
1. Add supervisor/nimbus/drpc gc log
1. Adjust jvm parameter 1. set -Xmn 1/2 of heap memory 2. set PermSize to 1/32 and MaxPermSize 1/16 of heap memory; 3. set -Xms by "worker.memory.min.size"。
1. Refine ZK error schema, when worker is dead, UI will report error
1. Add function to zktool utility, support remove all topology znodes, support list 
1. Optimize netty client.
1. Dynamic update connected task status by network connection, not by ZK znode.
1. Add configuration "topology.enable.metrics".
1. Classify all topology log into one directory by topologyName.

## Bug fix
1. Skip download same binary when assigment has been changed.
1. Skip start worker when binary is invalid.
1. Use correct configuration map in a lot of worker thread
1. In the first step Nimbus will check topologyName or not when submit topology
1. Support fieldGrouping for Object[]
1. For drpc single instance under one configuration
1. In the client topologyNameExists interface，directly use trhift api
1. Fix failed to restart due to topology cleanup thread's competition
1. Fix the bug that backpressure might be lost when trigger bolt was failed.
1. Fixed the bug that DefaultMetricUploader doesn't delete metrics data in rocksdb, causing new metrics data cannot be appended.

## Deploy and scripts
1. Optimize cleandisk.sh, avoid delete useful worker log

# Release 2.0.4-SNAPSHOT

## New features
1. Redesign Metric/Monitor system, new RollingWindow/Metrics/NettyMetrics, all data will send/recv through thrift
2. Redesign Web-UI, the new Web-UI code is clear and clean
3. Add NimbusCache Layer, using RocksDB and TimeCacheWindow
4. Refactoring all ZK structure and ZK operation
5. Refactoring all thrift structure
6. Merge jstorm-client/jstorm-client-extension/jstorm-core 3 modules into jstorm－core
7. Set the dependency version same as storm
8. Sync apache-storm-0.10.0-beta1 all java code
9. Switch log system to logback
10. Upgrade thrift to apache thrift 0.9.2
11. Performance tuning Huge topology more than 600 workers or 2000 tasks
12. Require jdk7 or higher

# Release 0.9.7.1

## New Features
1. Batch the tuples whose target task is same, before sending out（task.batch.tuple=true，task.msg.batch.size=4）.  
2. LocalFirst grouping is updated. If all local tasks are busy, the tasks of outside nodes will be chosen as target task instead of waiting on the busy local task.
3. Support user to reload the application config when topology is running.
4. Support user to define the task heartbeat timeout and task cleanup timeout for topology.
5. Update the wait strategy of disruptor queue to no-blocking mode "TimeoutBlockingWaitStrategy"
6. Support user to define the timeout of discarding messages that are pending for a long time in netty buffer.  
7. Update the message processing structure. The virtualPortDispatch and drainer thread are removed to reduce the unnecessary cost of cpu and the transmitting of tuples 
8. Add jstorm parameter "--include-jars" when submit topology, add these jar to classpath
9. Nimbus or Supervisor suicide when the local ip is 127.0.0.0
10. Add user-define-scheduler example
11. Merge Supervisor's syncSupervisor and syncProcess

## Bug Fix
1. Improve the GC setting.
2. Fix the bug that task heartbeat might not be updated timely in some scenarioes.  
3. Fix the bug that the reconnection operation might be stick for a unexpected period when the connection to remote worker is shutdown and some messages are buffer in netty.   
4. Reuse thrift client when submit topology
5. Avoid repeatedly download binary when failed to start worker.

## Changed setting
1. Change task's heartbeat timeout to 4 minutes
2. Set the netty client thread pool(clientScheduleService) size as 5 

## Deploy and scripts
1. Improve cleandisk.sh, avoid delete current directory and /tmp/hsperfdata_admin
2. Add executable attribute for the script under example
3. Add parameter to stat.sh, which can be used to start supervisor or not. This is useful under virtual 

# Release 0.9.7

## New Features
1. Support dynamic scale-out/scale-in of worker, spout, bolt or acker without stopping the service of topology.
2. When enable cgroup, Support the upper limit control of cpu core usage. Default setting is 3 cpu cores.
3. Update the mechanism of task heartbeats to make heartbeat to track the status of spout/bolt execute thread correctly.
4. Support to add jstorm prefix info(clusterName, topologyName, ip:port, componentName, taskId, taskIndex) for worker/task log
5. Check the heartbeat of supervisor when topology assignment to ensure no worker will be assigned into a dead supervisor
6. Add api to query the task/worker's metric info, e.g. load status of task queue, worker cpu usage, worker mem usage...
7. Try to re-download jars when staring worker fails several times to avoid potential corruption of jars 
8. Add Nimbus ZK cache, accelerate nimbus read zk
9. Add thrift api getVersion, it will be used check between the client jstorm version and the server jstorm version.  
10. Update the metrics' structure to Alimonitor
11. Add exclude-jar parameter into jstorm.py, which avoid class conflict when submit topology

## Bug Fix
1. Fix the no response problem of supervisor process when subimtting big amout topologys in a short time
2. When submitting two or more topologys at the same time, the later one might be failed.
3. TickTuple does not need to be acked. Fix the incorrect count of failure message.
4. Fix the potential incorrect assignment when use.old.assignment=true
5. Fix failed to remove some zk nodes when kill topology 
6. Fix failed to restart topology, when nimbus do assignment job.
7. Fix NPE when register metrics
8. Fix failed to read ZK monitor znode through zktool
9. Fix exception when enable classload and local mode
10. Fix duplicate log when enable user-defined logback in local mode

## Changed Setting
1. Set Nimbus jvm memory size as 4G
2. Set hearbeat from supervisor to nimbus timeout from 60s to 180s
3. In order to avoid OOM, set storm.messaging.netty.max.pending as 4
4. Set task queue size as 1024, worker's total send/receive queue size as 2048

## Deploy and scripts
1. Add rpm build spec
2. Add deploy files of jstorm for rpm package building
3. Enable the cleandisk cronjob every hour, reserve coredump for only one hour.

# Release 0.9.6.3

## New features
1. Implement tick tuple
2. Support logback
3. Support to load the user defined configuration file of log4j
4. Enable the display of user defined metrics in web UI
5. Add "topologyName" parameter for "jstorm list" command
6. Support the use of ip and hostname at the same for user defined schedule
7. Support junit test for local mode
8. Enable client command(e.g. jstorm jar) to load self-defined storm.yaml

## Bug fix
1. Add activate and deactivate api of spout, which are used in nextTuple prepare phase
2. Update the support of multi language
3. Check the worker's heartbeat asynchronously to speed up the lunch of worker
4. Add the check of worker's pid to speed up the detect of dead worker
5. Fix the high cpu load of disruptor producer when disruptor queue is full
6. Remove the confused exception reported by disruptor queue when killing worker
7. Fix the failure problem of "jstorm restart" client command
8. Report error when user submits the jar built on a incompatible jstorm release
9. Fix the problem that one log will printed twice when user define a configuration of log4j or logback on local mode
10. Fix the potential exception when killing topology on local mode
11. Forbid user to change the log level of jstorm log
12. Add a configuration template of logback
13. Fix the problem that process the upload of lib jar as application jar
14. Makesure the clean of ZK node for a topology which is removed
15. Add the information of topology name when java core dump
16. Fix the incorrect value of -XX:MaxTenuringThreshold. Currently, the default value of jstorm is 20, but the max value in JDK8 is 15.
17. Fix the potential reading failure of cpu core number, which may cause the supervisor slot to be set to 0
18. Fix the "Address family not supported by protocol family" error on local mode
19. Do not start logview http server on local mode
20. Add the creation of log dir in supervisor alive checking scription
21. Check the correctness of ip specified in configuration file before starting nimbus
22. Check the correctness of env variable $JAVA_HOME/$JSTORM_HOME/$JSTORM_CONF_DIR before starting jstorm service
23. Specify the log dir for rpm installation
24. Add reading permission of /home/admin/jstorm and /home/admin/logs for all users after rpm installation
25. Config local temporay ports when rpm installation
26. Add noarch rpm package

# Release 0.9.6.2
1. Add option to switch between BlockingQueue and Disruptor
2. Fix the bug which under sync netty mode, client failed to send message to server 
3. Fix the bug let web UI can dispaly 0.9.6.1 cluster
4. Fix the bug topology can be submited without main jar but a lot of little jar
5. Fix the bug restart command 
6. Fix the bug trident bug
7. Add the validation of topology name, component name... Only A-Z, a-z, 0-9, '_', '-', '.' are valid now.
8. Fix the bug close thrift client

# Release 0.9.6.2-rc
1. Improve user experience from Web UI
1.1 Add jstack link
1.2 Add worker log link in supervisor page
1.3 Add Web UI log encode setting "gbk" or "utf-8"
1.4 Show starting tasks in component page
1.5 Show dead task's information in UI
1.6 Fix the bug that error info can not be displayed in UI when task is restarting
2. Add restart command, with this command, user can reload configuration, reset worker/task parallism
3. Upgrade curator/disruptor/guava version
4. Revert json lib to google-simple json, wrap all json operation into two utility method
5. Add new storm submit api, supporting submit topology under java 
6. Enable launch process with backend method
7. Set "spout.pending.full.sleep" default value as true
8. Fix the bug user define sceduler not support a list of workers
9. Add disruptor/JStormUtils junit test
10. Enable user to configure the name of monitor name of alimonitor
11. Add tcp option "reuseAddress" in netty framework
12. Fix the bug: When spout does not implement the ICommitterTrident interface, MasterCoordinatorSpout will stick on commit phase.

# Release 0.9.6.2-rc
1. Improve user experience from Web UI
1.1 Add jstack link
1.2 Add worker log link in supervisor page
1.3 Add Web UI log encode setting "gbk" or "utf-8"
1.4 Show starting tasks in component page
1.5 Show dead task's information in UI
1.6 Fix the bug that error info can not be displayed in UI when task is restarting
2. Add restart command, with this command, user can reload configuration, reset worker/task parallism
3. Upgrade curator/disruptor/guava version
4. Revert json lib to google-simple json, wrap all json operation into two utility method
5. Add new storm submit api, supporting submit topology under java 
6. Enable launch process with backend method
7. Set "spout.pending.full.sleep" default value as true
8. Fix the bug user define sceduler not support a list of workers
9. Add disruptor/JStormUtils junit test
10. Enable user to configure the name of monitor name of alimonitor
11. Add tcp option "reuseAddress" in netty framework
12. Fix the bug: When spout does not implement the ICommitterTrident interface, MasterCoordinatorSpout will stick on commit phase.

# Release 0.9.6.1
1. Add management of multiclusters to Web UI. Added management tools for multiclusters in WebUI.
2. Merged Trident API from storm-0.9.3
3. Replaced gson with fastjson
4. Refactored metric json generation code.
5. Stored version info with $JSTORM_HOME/RELEASE.
6. Replaced SingleThreadDisruptorQueue with MultiThreadDisruptorQueue in task deserialize thread.
7. Fixed issues with worker count on Web UI.
8. Fixed issues with accessing the task map with multi-threads.
9. Fixed NullPointerException while killing worker and reading worker's hearbeat object.
10. Netty client connect to server only in NettyClient module.
11. Add break loop operation when netty client connection is closed
12. Fix the bug that topology warning flag present in cluster page is not consistent with error information present in topology page
13. Add recovery function when the data of task error information is corrupted
14. Fix the bug that the metric data can not be uploaded onto Alimonitor when ugrading from pre-0.9.6 to 0.9.6 and executing pkill java without restart the topologying
15. Fix the bug that zeroMq failed to receive data
16. Add interface to easily setting worker's memory
17. Set default value of topology.alimonitor.metrics.post to false
18. Only start NETTY_SERVER_DECODE_TIME for netty server
19. Keep compatible with Storm for local mode
20. Print rootId when tuple failed
21. In order to keep compatible with Storm, add submitTopologyWithProgressBar interface
22. Upgrade netty version from 3.2.7 to 3.9.0
23. Support assign topology to user-defined supervisors


# Release 0.9.6
1. Update UI 
  - Display the metrics information of task and worker
  - Add warning flag when errors occur for a topology
  - Add link from supervisor page to task page
2. Send metrics data to Alimonitor
3. Add metrics interface for user
4. Add task.cleanup.timeout.sec setting to let task gently cleanup
5. Set the worker's log name as topologyName-worker-port.log
6. Add setting "worker.redirect.output.file", so worker can redirect System.out/System.err to one setting file
7. Add storm list command
8. Add closing channel check in netty client to avoid double close
9. Add connecting check in netty client to avoid connecting one server twice at one time 

# Release 0.9.5.1
1. Add netty sync mode
2. Add block operation in netty async mode
3. Replace exception with Throwable in executor layer
4. Upgrade curator-framework version from 1.15 to 1.3.2
5. Add more netty junit test
6. Add log when queue is full

# Release 0.9.5

## Big feature:
1. Redesign scheduler arithmetic, basing worker not task .

## Bug fix
1. Fix disruptor use too much cpu
2. Add target NettyServer log when f1ail to send data by netty

# Release 0.9.4.1

## Bug fix:
1. Improve speed between tasks who is running in one worker
2. Fix wrong timeout seconds
3. Add checking port when worker initialize and begin to kill old worker
4. Move worker hearbeat thread before initializing tasks
5. Move init netty-server before initializeing tasks 
6. Check whether tuple's rootId is duplicated
7. Add default value into Utils.getInt
8. Add result function in ReconnectRunnable
9. Add operation to start Timetick
10. Halt process when master nimbus lost ZK node
11. Add exception catch when cgroups kill process
12. Speed up  reconnect to netty-server
13. Share one task hearbeat thread for all tasks
14. Quickly haltprocess when initialization failed.
15. Check web-ui logview page size 



# Release 0.9.4

## Big features
1. Add transaction programming mode
2. Rewrite netty code, 1. use share boss/worker thread pool;2 async send batch tuples;3 single thread to do reconnect job;4 receive batch tuples
3. Add metrics and statics
4. Merge Alimama storm branch into this version, submit jar with -conf, -D, -lib


## Enhancement
1. add setting when supervisor has been shutdown, worker will shutdown automatically
2. add LocalFristGrouping api
3. enable cgroup for normal user



## Bug fix:
1. Setting buffer size  when upload jar
2. Add lock between ZK watch and timer thread when refresh connection
3. Enable nimbus monitor thread only when topology is running in cluster mode
4. Fix exception when failed to read old assignment of ZK
5. classloader fix when both parent and current classloader load the same class
6. Fix log view null pointer exception

# Release 0.9.3.1

## Enhancement
1. switch apache thrift7 to storm thrift7
2. set defatult acker number is 1
3. add "spout.single.thread" setting
4. make nimbus logview port different from supervisor's
5. web ui can list all files of log's subdir
6. Set gc dump dir as log's dir


# Release 0.9.3

## New feature
1. Support Aliyun Apsara/Hadoop Yarn

## Enhancement
1. Redesign Logview
2. Kill old worker under the same port when worker is starting
3. Add zk information/version information on UI
4. Add nodeport information for dead task in nimbus
5. Add interface to get values when spout doing ack
6. Add timeout statics in bolt
7. jstorm script return status
8. Add logs when fail to deserialize tuple 
9. Skip sleep operation when max_pending is 1 and waiting ack
10. Remove useless dependency
11. Longer task timeout setting
12. Add supervisor.use.ip setting
13. Redirect supervisor out/err to /dev/null, redirect worker out/err to one file


## Bug Fix
1. Fix kryo fail to deserialize object when enable classloader
2. Fix fail to reassign dead task when worker number is less than topology apply
3. Set samller jvm heap memory for jstorm-client 
4. Fix fail to set topology status as active when  do rebalance operation twice at one time,
5. Fix local mode bug under linux
6. Fix average latency isn't accurate
7. GC tuning.
8. Add default kill function for AysncLoopRunnable
 


# Release 0.9.2

## New feature
1. Support LocalCluster/LocalDrpc mode, support debugging topology under local mode
2. Support CGroups, assigning CPU in hardware level.
3. Support simple logview

## Bug fix or enhancement
1. Change SpoutExecutor's RotatingMap to TimeCacheMap, when putting too much timeout tuple is easy to cause deadlock in spout acker thread
2. Tunning gc parameter, improve performance and avoid full GC
3. Improve Topology's own gc priority, make it higher than JStorm system setting.
4. Tuning Nimbus HA, switch nimbus faster, when occur nimbus failure.
5. Fix bugs found by FindBugs tool.
6. Revert Trident interface to 0.8.1, due to 0.8.1's trident interface's performance is better.
7. Setting nimbus.task.timeout.secs as 60 to avoid nimbus doing assignment when task is under full gc.
8. Setting default rpc framework as netty
9. Tunning nimbus shutdown flow
10. Tunning worker shutdown flow
11. Add task heartbeat log
12. Optimize Drpc/LocalDrpc source code.
13. Move classloader to client jar.
14  Fix classloader fail to load  anonymous class
15. Web Ui display slave nimbus
16. Add thrift max read buffer size
17. Setting CPU slot base double
18. Move Zk utility to jstorm-client-extension.jar
19. Fix localOrShuffle null pointer
20. Redirecting worker's System.out/System.err to file is configurable.
21. Add new RPC frameworker JeroMq
22. Fix Zk watcher miss problem
23. Update sl4j 1.5.6 to 1.7.5
24. Shutdown worker when occur exception in Smart thread
25. Skip downloading useless topology in Supervisor
26. Redownload the topology when failed to deserialize topology in Supervisor.
27. Fix topology codeDir as resourceDir
28. Catch error when normalize topology
29. Add log when found one task is dead
30. Add maven repository, JStorm is able to build outside of Alibaba
31. Fix localOrShuffle null pointer exception
32. Add statics counting for internal tuples in one worker
33. Add thrift.close after download topology binary in Supervisor


# Release 0.9.1

## new features
1. Application classloader. when Application jar is conflict with jstorm jar, 
   please enable application classloader.
2. Group Quato, Different group with different resource quato.

## Bug fix or enhancement
1. Fix Rotation Map competition issue.
2. Set default acker number as 0
3. Set default spout/bolt number as 1
4. Add log directory in log4j configuration file
5. Add transaction example
6. Fix UI showing wrong worker numbe in topology page
7. Fix UI showing wrong latency in topology page
8. Replace hardcode Integer convert with JStormUtils.parseInt
9. Support string parse in Utils.getInt
10. Remove useless dependency in pom.xml
11. Support supervisor using IP or special hostname
12. Add more details when no resource has been assigned to one new topology
13. Replace normal thread with Smart thread
14. Add gc details 
15. Code format
16. Unify stormId and topologyId as topologyId
17. Every nimbus will regist ip to ZK



# Release 0.9.0
In this version, it will follow storm 0.9.0 interface, so the application running
on storm 0.9.0 can run in jstorm 0.9.0 without any change.

## Stability
1. provide nimbus HA. when the master nimbus shuts down, it will select another
 online nimbus to be the master. There is only one master nimbus online 
 any time and the slave nimbuses just synchronouse the master's data.
2. RPC through netty is stable, the sending speed is match with receiving speed. 


## Powerful scheduler
1. Assigning resource on four dimensions:cpu, mem, disk, net
2. Application can use old assignment.
3. Application can use user-define resource.
4. Task can apply extra cpu slot or memory slot.
4. Application can force tasks run on different supervisor or the same supervisor


# Release 0.7.1
In this version, it will follow storm 0.7.1 interface, so the topology running
in storm 0.7.1 can run in jstorm without any change.

## Stability
* Assign workers in balance
* add setting "zmq.max.queue.msg" for zeromq
* communication between worker and tasks without zeromq
* Add catch exception operation
  * in supervisor SyncProcess/SyncSupervisor
  * add catch exception and report_error in spout's open and bolt's prepare
  * in all IO operation
  * in all serialize/deserialize
  * in all ZK operation
  *  in topology upload/download function
  *  during initialization zeromq
* do assignmen/reassignment operation in one thread to avoid competition
* redesign nimbus 's topology assign algorithm, make the logic simple much.
* redesign supervisor's sync assignment algorithm, make the logic simple much
* reduce zookeeper load
  * redesign nimbus monitor logic, it will just scan tasks' hearbeat, frequency is 10s
  * nimbus cancel watch on supervisor
  * supervisor heartbeat frequence change to 10s
  * supervisor syncSupervisor/syncProcess frequence change to 10s
  * supervisor scan /$(ZKROOT)/assignment only once in one monitor loop
  * task hearbeat change to 10s
* create task pid file before connection zk, this is very import when zk is unstable.


## Performance tuning
* reduce once memory copy when deserialize tuple, improve performance huge.
* split executor thread as two thread, one handing receive tuples, one sending tuples, improve performance much
* redeisign sample code, it will sampling every 5 seconds, not every 20 tuple once, improve performance much
* simplify the ack's logic, make acker more effeciency
* Communication between worker and tasks won't use zeromq, just memory share in process
* in worker's Drainer/virtualportdispatch thread, spout/bolt recv/send thread, 
   the thread will sleep 1 ms when there is not tuple in one loop
* communication between worker and tasks without zeromq
* sampling frequence change to 5s, not every 20 tuple once.

## Enhancement:
* add IFailValueSpout interface
* Redesign sampling code, collection statics model become more common.
  *  Add sending/recving tps statics, statics is more precise.
* Atomatically do deactivate action when kill/rebalance topology, and the wait time is 2 * MSG_TIMEOUT
* fix nongrouping bug, random.nextInt will generate value less than 0.
* Sleep one setting time(default is 1 minute) after finish spout open, 
   which is used to wait other task finish initialization.
* Add check component name when submit topology, forbidding the component 
   which name start with "__"
* change the zk's node /$(ZKROOT)/storm to /$(ZKROOT)/topology
* abstract topology check logic from generating real topology function
* when supervisor is down and topology do rebalance, the alive task under down 
   supervisor is unavailable.
* add close connection operation after finish download topology binary
* automatically create all local dirtorie, such as 
   /$(LOCALDIR)/supervisor/localstate
* when killing worker, add "kill and sleep " operation before "kill -9" operation
* when generate real topology binary,
  * configuration priority different.   
      component configuration > topology configuration > system configuration
  * skip the output stream which target component doesn't exist.
  * skip the component whose parallism is 0.
  * component's parallism is less than 0, throw exception.
* skip ack/fail when inputstream setting is empty
* add topology name to the log
* fix ui select option error, default is 10 minutes
* supervisor can display all worker's status