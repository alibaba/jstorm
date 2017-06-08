[JStorm English introduction](http://42.121.19.155/jstorm/JStorm-introduce-en.pptx)
[JStorm Chinese introduction](http://42.121.19.155/jstorm/JStorm-introduce.pptx)

# Release 2.4.0

## New features
* exactly-once接口支持异步checkpoint(rocksdb+hdfs)
* 引入重构的窗口机制
    1. 支持滚动和滑动窗口
    2. 支持常见窗口类型,包括:count window, processing time window, event time window, session window.
    3. 不会在内存中憋数据,有数据到达即会触发计算
* 支持灰度发布
    1. 支持根据worker/component来进行灰度发布
    2. 支持灰度发布的回滚
* 支持基于内存/rocksdb的KV store
* HBase metrics插件开源
* 支持多个metrics uploader同时写异构存储
* MetricClient支持注册汇总到topology级别的metrics
* 支持component stream metrics, 即stream metrics在component级别的汇总 

## Improvements
* 支持kryo中没有无参构造函数类的序列化
* 在AsmMetric中加入getValue方法,用于单测/集成测试中快速assert,而不必通过nimbus client从nimbus中获取数据

## Bug Fix
* 修复调度topology的时候unstopped task的计算错误
* 修复supervisor中storm.yaml永远跟nimbus的storm.yaml不同,从而一直在同步的bug
* 修复kryo配置中无法识别字符串"true"/"false"配置值的bug


# Release 2.2.1

## New features
* 性能优化:对比2.1.1和去年的双十一版本0.9.8.1有200%~300%的提升。在高并发和低并发的多个测试场景(word count)中，是Flink性能的120%~200%，是Storm的300%~400%。
	1. 重构batch的实现方案
	2. 优化序列化和反序列过程，减少cpu和网络消耗
	3. 优化消息关键路径和metrics的cpu开销
	4. 优化网络接收和发送端的处理策略
	5. 增加disruptorQueue的异步batch操作
* 加入新的snapshot exactly once(只处理一次)框架。
	1. 对比原有的Trident解决方案有着数倍的性能提升。同时可以减少用户在回滚的过程中的处理逻辑。
	2. 同时支持at least once(至少处理一次场景)。对比原有的acker机制，可以减少acker的消息处理开销，同时在高吞吐的场景中可以大量的减少acker消息占用的网络带宽。以提高任务性能。
* 完成JStorm on yarn支持。
	1. 现在JStorm可以实现快速的集群部署，以及集群的扩容和缩容。有效的提高集群资源的弹性和利用率。
* 重构backpressure设计，支持stage by stage的流控模式。
	1. 当前的设计更加轻量，让backpressure在流控开启和关闭时更加高效。
	2. 性能和稳定性对比原因的方案有着很大的提升。
* 引入Window API。
	1. 支持tumbling window，sliding window
	2. 对应的window支持count和duration 模式
	3. 支持window的watermark机制
* 引入对Flux的支持
	1. Flux是帮助创建和部署storm拓扑的编程框架及通用组件。帮助用户更方便创建及部署JStorm流式计算拓扑的编程框架
* 通过maven shade的方式，对一些容易冲突的依赖包做shade。以解决jstorm依赖和用户依赖之前的冲突问题。
* 优化Shuffle grouping方案
	1. 合并shuffle， localOrShuffle和localFirst。根据任务情况自动适配。
	2. shuffle时会根据下游节点的负载情况，做shuffle。以达到负载均衡。
* 增加Nimbus的黑名单机制。
* 增加Trident对消息batch模式的支持
* 支持集群的全局配置推送
* supervisor info和心跳中增加了buildTs，便于区分出集群中是否存在不同版本的supervisor
* nimbus和supervisor通过ext模块来支持外部插件
* 添加elastic search 5.11的支持, 感谢 @elloray 的PR

## Improvements
* 重构nimbus metrics 框架，将原TopologyMetricsRunnable打散成事件驱动
* 重构Topology master的处理逻辑。改为事件驱动。提高Topology的处理性能。
* 重构example 代码， 增加大量example和测试用例
* 默认禁用stream metrics以及其他特定metrics，以减少发送的数据量
* 本地模式下启用metrics
* gauge的实现，由每分钟单值，改为每分钟采样多次计算平均值
* 引入了一种近似计算的方式来计算histogram的值，以减少内存开销
* 增加了Full GC以及supervisor中网络相关的metrics

## Bug Fix
* Fix 消息的乱序问题
* Fix supervisor上有大量的zookeeper连接的问题
* Fix task初始化时，deactivate的错误调用
* Fix spout并发高时，少量消息rootid重复，导致消息失败的问题。
* Fix 本地模式的一些bug
* Fix logwriter的bug
* 修复了task metrics中RecvTps, ProcessLatency没有合并到task的bug
* 修复了AsmCounter在flush时的线程同步问题


# Release 2.1.1

## New features
* 相比2.1.0有1.5～6倍的性能提升
* 添加了应用层的自动batch
* 增加了独立的控制消息通道，将控制消息与业务消息分开，保证控制消息有较高的优先级被处理
* 支持jdk1.8
* 添加了nimbus hook和topology hook
* Metrics系统:
    * 支持动态开关特定的metrics
    * 添加了metrics的设计文档，见JSTORM-METRICS.md
* JStorm web UI相关:
    * 添加了zk节点的查看功能，感谢@dingjun84的PR
    * 添加了普通日志搜索以及topology日志搜索功能，支持正向和反向搜索
    * 支持日志的下载
* 支持动态修改日志级别
* 修改了zk中ErrorInfo的结构，增加了errorLevel, errorCode和duration
* 增加了supervisor的健康检查
* 增加了-Dexclude.jars选项以手动排除特定的jar包

## Improvements
* Metrics相关：
    * 使用JHistogram/JMeter代替codahale的Histogram/Meter, 把内部的Clock.tick改为System.currentTimeMillis，Meter和Histogram分别有50%和25%+的性能提升
    * 添加了TupleLifeCycle指标，用于统计消息从当前component发出来到下一个component被处理的总耗时
    * 添加了supervisor的指标: total_cpu_usage, total_mem_usage, disk_usage
    * 删除了一些不必要的指标，如emitTime等
    * 使用HeapByteBuffer代替List<Long>来发送histogram中的点数据，节省60+%的metrics内存使用 
    * 将metrics采样率从10%调为5%
    * 删除AsmTimer及相关代码
* 日志相关：
    * 默认不再使用log4j，而是使用logback，除非slf4j-log4j12依赖
    * 使用jstorm.log.dir配置取代原来的${jstorm.home}/logs, 详见jstorm.logback.xml 
    * 将logback/log4j中日志文件的默认编码改为UTF-8
    * 修改日志目录结构，添加了${topology.name}目录, 详见jstorm.logback.xml
    * 将代码中所有log4j的Logger改为slf4j的Logger
    * 将defaults.yaml中默认的日志page size(log.page.size)由32K调整为128K
    * 将supervisor/nimbus的gc日志文件加上时间戳，防止重启后被覆盖; 重启worker前备份worker之前的gc日志
* 优化反压策略，防止过度反压
* 将acker中的pending map改为单线程，以提高性能
* 修改RefreshConnections逻辑，防止频繁地从zk中下载assignments
* 将Supervisor的默认内存由512MB调到1GB
* 统一使用ProcessLauncher来起进程
* 为supervisor和nimbus添加DefaultUncaughtExceptionHandler
* 与0.9.x系列错开端口配置，见supervisor.slots.ports.base, nimbus.thrift.port, nimbus.deamon.logview.port, supervisor.deamon.logview.port配置
* 将web UI的前端库highcharts改为echarts，防止潜在的版权冲突
* 依赖升级
    * 升级kryo至2.23.0
    * 升级disruptor至3.2.2

## Bug fix
* 修复了起worker时可能的死锁
* 修复了当localstate文件为空时，supervisor无法启动的问题
* 修复了开启kryo时metrics中HeapByteBuffer无法被注册的bug
* 修复了内存使用率计算不准确的bug
* 修复了当用户自定义调度中分配的worker大于真实worker数时会创建空worker的bug
* 修复web UI的日志目录设置错误的问题 
* 修复了web UI中的XSS bug
* 在local mode的时候，TopologyMetricsRunnable线程不运行，感谢@L-Donne的PR
* 修复JSTORM-141, JSTORM-188：TopologyMetricsRunnable消耗CPU过多的bug
* 删除MaxTenuringThreshold JVM参数
* 修复MkLocalShuffer中outTasks可能为null的bug

## Deploy and scripts
* 增加了对core dump文件的清理
* 增加了supervisor的健康检测，见healthCheck.sh
* 修改了jstorm.py，起进程后结束父python进程

## 升级指南
* JStorm2.1.1基本与JStorm2.1.0保持兼容，但是为了保险起见，建议重启topology
* 如果你的topology原先使用log4j，请在你的conf或者storm.yaml中加入"user.defined.log4j.conf: jstorm.log4j.properties"配置，以保证JStorm框架使用log4j
* 如果你使用slf4j-api + log4j，请在你的应用中添加slf4j-log4j12的依赖


# Release 2.1.0

## New features
* 完全重构web ui
	*	 大量美化界面
	*	 大幅提高web ui展示速度
	*	 增加topology和集群基本的最近30分钟的汇总信息
	*	 增加拓扑图， 并增加一些交互功能来直观获取拓扑的一些关键信息(例如emit， tuple lifecycle time， tps)
* 重构采样系统, 全新采样引擎和监控系统
	*	 新采样不再存储数据到zk
	*	 底层采样引擎更新， 支持抗噪处理, 合并计算更加方便
	*	 支持metrics的高可用
	*	 增加tuple生命周期, netty，disk空间 采样， worker内存采样更准确
	*	 支持外接数据库插件存储监控数据
* 实现智能反压(backpressure) 功能
	*	 自动进行限流控制
	*	 可以手动人工干预限流控制状态
* 实现中央控制单元TopologyMaster
	*	 重构心跳检查机制， 支持6000+ task
	*	 收集所有metrics，并做合并计算
	*	 中央控制流协调器
	*	 HA 状态存储
* 重新定义zk 数据结构和使用方式， 使一套zookeeper可以支撑2000+物理机器
	*	 不再存储任何动态数据
	*	 nimbus 获取topology／supervisor／cluster info时， 减少对zk访问次数
	*	 合并大量task级别znode，降低对zk的访问
	*	 优化task error节点，降低对zk的访问
	*	 优化zk cache操作
* 优化应用层batch功能， 提高性能
	*	 增加自动调整batch size功能
	*	 修复内存拷贝问题
	*	 内部通道数据，无需batch
	*	 默认kryo序列化
* 增加动态binary更新功能和配置更新功能 
* localShuffle 功能优化，提高性能，本worker，本节点，其他节点 3级shuffle，并动态探测队列负荷， 网络连接状态。
* 默认打开kryo， 提高性能
* 优化nimbus HA 机制， 优先级最高的nimbus 才能被promote成master，增加稳定性



## 优化
* supervisor自动dump worker jstack和jmap, 当worker处于invalid状态时.
* supervisor可以对内存超卖设置
* supervisor增加topology相关文件的下载重试机制。
* 增加配置logdir设置
* 增加配置，可使nimbus机器不自动启动supervisor
* 增加supervisor/nimbus/drpc gc日志
* 优化jvm参数  1. set -Xmn 1/2 of heap memory 2. set PermSize to 1/32 and MaxPermSize 1/16 of heap memory; 3. 增加最小内存设置-Xms "worker.memory.min.size"。
* ZK error 重新定义， 并且当worker死去时会，会在web ui报错, 
* 更新zktool，支持清理不干净的topology，并支持list功能
* 优化netty client和zk连接重试的时间间隔获取机制
* 修改out task状态更新机制，从zk上读取心跳信息，改为根据网络连接状态。以减少zk的依赖
* 增加配置参数 topology.enable.metrics: true/false, 用来启用或禁用metric
* 日志归类，相同topologyName的日志归类到对应目录下


## Bug fix
* Fix supervisor在调度有变化时，重复的下载任务jar包
* Fix supervisor下载失败，不会尝试用错误的jar去启动worker
* 大量的线程使用了错误的conf， 应该使用worker的conf
* 提交拓扑时，服务端会首先检测拓扑名字的合法性
* Fix fieldGrouping方式之前对Object[]数据结构不支持
* Fix 使drpc 单例模式
* 客户端topologyNameExists改进，直接使用trhift api
* Fix restart 过程中， 因定时清理线程清理导致的restart失败
* Fix 当trigger bolt失败时,反压可能丢失
* Fix DefaultMetricUploader没有删除rocksdb中的数据,导致新的metrics数据无法添加


## 运维和脚本
* 优化cleandisk.sh脚本, 防止误删worker日志

# Release 2.0.4-SNAPSHOT

## New features
1. 完全重构采样系统， 使用全新的Rollingwindow和Metric计算方式，尤其是netty采样数据，另外metric 发送和接收将不通过zk
2. 完全重构web-ui
3. 引入rocketdb，增加nimbus cache layer
4. 梳理所有的zk节点和zk操作， 去掉无用的zk 操作
5. 梳理所有的thrift 数据结构和函数， 去掉无用的rpc函数
6. 将jstorm-client/jstorm-client-extension/jstorm-core整合为jstorm－core
7. 同步依赖和storm一样
8. 同步apache-storm-0.10.0-beta1 java 代码
9. 切换日志系统到logback
10. 升级thrift 到apache thrift 0.9.2
11. 针对超大型任务600个worker／2000个task以上任务进行优化
12. 要求 jdk7 or higher

# Release 0.9.7.1

## New features
1. 增加Tuple自动batch的支持，以提高TPS以及降低消息处理延迟（task.batch.tuple=true，task.msg.batch.size=4）
2. localFirst在本地节点处理能力跟不上时，自动对外部节点进行扩容
3. 任务运行时，支持对任务配置的动态更新
4. 支持任务对task心跳和task cleanup超时时间的自定义设置
5. 增加disruptor queue对非阻塞模式TimeoutBlockingWaitStrategy的支持
6. 增加Netty层消息发送超时时间设置的支持，以及Netty Client配置的优化
7. 更新Tuple消息处理架构。去除不必要的总接收和总发送队列，减少消息流动环节，提高性能以及降低jstorm自身的cpu消耗。
8. 增加客户端"--include-jars"， 提交任务时，可以依赖额外的jar
9. 启动nimbus/supervisor时， 如果取得的是127.0.0.0地址时， 拒绝启动
10. 增加自定义样例
11. 合并supervisor 的zk同步线程syncSupervisor和worker同步线程syncProcess

## 配置变更
1. 默认超时心跳时间设置为4分钟
2. 修改netty 线程池clientScheduleService大小为5

## Bug fix
1. 优化gc参数，4g以下内存的worker默认4个gc线程，4g以上内存， 按内存大小/1g * 1.5原则设置gc线程数量
2. Fix在bolt处理速度慢时，可能出现的task心跳更新不及时的bug
3. Fix在一些情况下，netty连接重连时的异常等待bug
4. 提交任务时， 避免重复创建thrift client
5. Fix 启动worker失败时，重复下载binary问题

## 运维和脚本
1. 优化cleandisk.sh脚本， 防止把当前目录删除和/tmp/hsperfdata_admin/
2. 增加example下脚本执行权限
3. 添加参数supervisor.host.start: true/false，可以通过脚本start.sh批量控制启动supervisor或不启动supervisor，默认是启动supervisor

# Release 0.9.7

## New features
1. 实现topology任务并发动态调整的功能。在任务不下线的情况下，可以动态的对worker，spout, bolt或者ack进行扩容或缩容。rebalance命令被扩展用于支持动态扩容/缩容功能。
2. 当打开资源隔离时，增加worker对cpu核使用上限的控制
3. 调整task心跳更新机制。保证能正确反映spout/bolt exectue主线程的状态。
4. 对worker和task的日志，增加jstorm信息前缀(clusterName, topologyName, ip:port, componentName, taskId, taskIndex)的支持
5. 对topology任务调度时，增加对supervisor心跳状态的检查，不往无响应的supervisor调度任务
6. 增加metric查询API，如: task的队列负载情况，worker的cpu，memory使用情况
7. 增加supervisor上对任务jar包下载的重试，让worker不会因为jar在下载过程中的损坏，而启动失败
8. 增加ZK Cache功能, 加快zk 读取速度, 并对部分节点采取直读方式
9. 增加thrift getVersion api， 当客户端和服务器端版本不一致是，报warning
10. 增加supervisor 心跳检查， 会拒绝分配任务到supervisor心跳超时的supervisor
11. 更新发送到Alimonitor的user defined metrics 数据结构
12. 增加客户端exclude-jar 功能， 当客户端提交任务时，可以通过exclude-jar和classloader来解决jar冲突问题。

## 配置变更
1. 修改supervisor到nimbus的心跳 超时时间到180秒
2. 为避免内存outofmemory， 设置storm.messaging.netty.max.pending默认值为4
3. 设置Nimbus 内存至4G
4. 调大队列大小 task 队列大小为1024， 总发送队列和总接收队列为2048

## Bug fix
1. 短时间能多次restart worker配置多的任务时，由于Nimbus thrift thread的OOM导致，Supervisor可能出现假死的情况
2. 同时提交任务，后续的任务可能会失败
3. tickTuple不需要ack，更正对于tickTuple不正确的failed消息统计
4. 解决use.old.assignment=true时，默认调度可能出现错误 
5. 解决删除topology zk 清理不干净问题
6. 解决当做任务分配时， restart topology失败问题
7. 解决同时提交多个topology 竞争问题
8. 解决NPE 当注册metrics 
9. 解决 zkTool 读取 monitor的 znode 失败问题
10.解决 本地模式和打开classloader模式下， 出现异常问题
11.解决使用自定义日志logback时， 本地模式下，打印双份日志问题

## 运维& 脚本
1. Add rpm build spec
2. Add deploy files of jstorm for rpm package building
3. cronjob改成每小时运行一次， 并且coredump 改成保留1个小时

# Release 0.9.6.3

## New features
1. 实现tick tuple
2. 支持logbak
3. 支持加载用户自定义Log4j 配置文件
4. Web UI显示用户自定义metrics
5. jstorm list 命令支持topologyName
6. 所有底层使用ip，自定义调度的时候，支持自定义调度中ip和hostname混用
7. 本地模式支持junit test
8. 客户端命令（比如提交jar时）可以指定storm.yaml 配置文件

## Bug fix
1. 在spout 的prepare里面增加active动作
2. 多语言支持
3. 异步检查worker启动心跳，加快worker启动速度
4. 进程pid检查，加快发现worker已经死去的速度
5. 使用错误的disruptor 类， 当disruptor队列满时，producer狂占CPU
6. kill worker时， disruptor 报错，引起误解
7. restart 命令可能失败
8. JStorm 升级后，客户端提交一不兼容版本的应用jar时， 需要正确报错
9. 本地模式时， 如果用户配置了log4j或logback时， 会打印日志2次
10. 本地模式时， 应用调用了killTopology时， 可能出现exception
11. 避免应用hack logger, 需要额外设置jstorm 的log level为info
12. 增加一个logback 配置文件模板
13. 上传lib jar时， 会有可能把lib jar当作应用的jar来处理了
14. 删除一个topology后， 发现zk还是有一些node没有清理干净
15. java core dump时，必须带上topology的名字
16. JDK8 里-XX:MaxTenuringThreshold 的最大值是15，默认配置里的是20
17. 一些特殊情况下，无法获取cpu 核数，导致supervisor slot数为0
18. Fix 本地模式时，zk 报"Address family not supported by protocol family"
19. Fix 本地模式时，关闭logview http server
20. 在检查supervisor是否存活脚本中，创建日志目录
21. 启动脚本对nimbus ip 的完整word检查，避免误启动其他机器nimbus
22. 启动脚本对环境变量$JAVA_HOME/$JSTORM_HOME/$JSTORM_CONF_DIR检查, 并自动加载bash配置文件
23. rpm安装包中，修改日志目录为/home/admin/logs
24. rpm安装后，需要让/home/admin/jstorm, /home/admin/logs 可以被任意用户读取
25. rpm安装包中，设置本地临时端口区间
26. 需要一个noarch的rpm包

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