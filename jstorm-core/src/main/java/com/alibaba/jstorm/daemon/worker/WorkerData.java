
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.worker;

import backtype.storm.Config;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.messaging.ControlMessage;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoByteBufferSerializer;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.*;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.common.metric.FullGcGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LogUtils;
import com.alibaba.jstorm.zk.ZkTool;
import com.codahale.metrics.Gauge;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.jstorm.schedule.Assignment.AssignmentType;

public class WorkerData {
    private static Logger LOG = LoggerFactory.getLogger(WorkerData.class);

    public static final int THREAD_POOL_NUM = 4;
    private ScheduledExecutorService threadPool;

    // system configuration

    private Map<Object, Object> conf;
    // worker configuration

    private Map<Object, Object> stormConf;

    // message queue
    private IContext context;

    private final String topologyId;
    private final String supervisorId;
    private final Integer port;
    private final String workerId;
    // worker status :active/shutdown
    private AtomicBoolean shutdown;
    private AtomicBoolean monitorEnable;

    // Topology status
    private StatusType topologyStatus;

    // ZK interface
    private ClusterState zkClusterState;
    private StormClusterState zkCluster;

    // running taskId list in current worker
    private Set<Integer> taskIds;

    // connection to other workers <NodePort, ZMQConnection>
    private ConcurrentHashMap<WorkerSlot, IConnection> nodePortToSocket;
    // <taskId, NodePort>
    private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;

    private ConcurrentSkipListSet<ResourceWorkerSlot> workerToResource;

    private volatile Set<Integer> outboundTasks;
    private Set<Integer> localNodeTasks = new HashSet<>();

    private ConcurrentHashMap<Integer, DisruptorQueue> innerTaskTransfer;
    private ConcurrentHashMap<Integer, DisruptorQueue> controlQueues;
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    // <taskId, component>
    private ConcurrentHashMap<Integer, String> tasksToComponent;
    private ConcurrentHashMap<String, List<Integer>> componentToSortedTasks;

    private Map<String, Object> defaultResources;
    private Map<String, Object> userResources;
    private Map<String, Object> executorData;
    private Map registeredMetrics;

    // raw topology is deserialized from local jar which doesn't contain ackers
    private StormTopology rawTopology;
    // sys topology is the running topology in the worker which contains ackers
    private StormTopology sysTopology;

    private ContextMaker contextMaker;

    // shutdown worker entrance
    private final AsyncLoopDefaultKill workHalt = new AsyncLoopDefaultKill();

    // sending tuple's queue
    // private LinkedBlockingQueue<TransferData> transferCtrlQueue;
    private DisruptorQueue transferCtrlQueue;

    private List<TaskShutdownDameon> shutdownTasks;

    private ConcurrentHashMap<Integer, Boolean> outTaskStatus; // true => active

    private FlusherPool flusherPool;

    private volatile Long assignmentTS; // assignment timeStamp. last update time of assignment

    private volatile AssignmentType assignmentType;

    private IConnection recvConnection;

    private JStormMetricsReporter metricReporter;

    @SuppressWarnings("unused")
    private AsyncLoopThread healthReporterThread;

    private AtomicBoolean workerInitConnectionStatus;

    private AtomicReference<KryoTupleSerializer> atomKryoSerializer = new AtomicReference<>();

    private AtomicReference<KryoTupleDeserializer> atomKryoDeserializer = new AtomicReference<>();

    // the data/config which need dynamic update
    private UpdateListener updateListener;

    protected List<AsyncLoopThread> deserializeThreads = new ArrayList<>();
    protected List<AsyncLoopThread> serializeThreads = new ArrayList<>();

    @SuppressWarnings({"rawtypes", "unchecked"})
    public WorkerData(Map conf, IContext context, String topologyId, String supervisorId,
                      int port, String workerId, String jarPath) throws Exception {
        this.conf = conf;
        this.context = context;
        this.topologyId = topologyId;
        this.supervisorId = supervisorId;
        this.port = port;
        this.workerId = workerId;

        this.shutdown = new AtomicBoolean(false);

        this.monitorEnable = new AtomicBoolean(true);
        this.topologyStatus = null;

        this.workerInitConnectionStatus = new AtomicBoolean(false);

        if (StormConfig.cluster_mode(conf).equals("distributed")) {
            String pidDir = StormConfig.worker_pids_root(conf, workerId);
            JStormServerUtils.createPid(pidDir);
        }

        // create zk interface
        this.zkClusterState = ZkTool.mk_distributed_cluster_state(conf);
        this.zkCluster = Cluster.mk_storm_cluster_state(zkClusterState);

        Map rawConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
        this.stormConf = new HashMap<>();
        this.stormConf.putAll(conf);
        this.stormConf.putAll(rawConf);

        // init topology.debug and other debug args
        JStormDebugger.update(stormConf);
        // register dynamic updaters
        registerUpdateListeners();

        JStormMetrics.setHistogramValueSize(ConfigExtension.getTopologyHistogramSize(stormConf));
        JStormMetrics.setTopologyId(topologyId);
        JStormMetrics.setPort(port);
        JStormMetrics.setDebug(ConfigExtension.isEnableMetricDebug(stormConf));
        JStormMetrics.enabled = ConfigExtension.isEnableMetrics(stormConf);
        JStormMetrics.enableStreamMetrics = ConfigExtension.isEnableStreamMetrics(stormConf);
        JStormMetrics.addDebugMetrics(ConfigExtension.getDebugMetricNames(stormConf));
        AsmMetric.setSampleRate(ConfigExtension.getMetricSampleRate(stormConf));

        ConfigExtension.setLocalSupervisorId(stormConf, this.supervisorId);
        ConfigExtension.setLocalWorkerId(stormConf, this.workerId);
        ConfigExtension.setLocalWorkerPort(stormConf, port);
        ControlMessage.setPort(port);

        JStormMetrics.registerWorkerTopologyMetric(
                JStormMetrics.workerMetricName(MetricDef.CPU_USED_RATIO, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getCpuUsage();
                    }
                }));

        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.HEAP_MEMORY_USED, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getJVMHeapMemory();
                    }
                }));

        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.MEMORY_USED, MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return JStormUtils.getMemUsage();
                    }
                }));


        JStormMetrics.registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.FULL_GC, MetricType.GAUGE),
                new AsmGauge(new FullGcGauge()) {
                    @Override
                    public AsmMetric clone() {
                        AsmMetric metric = new AsmGauge((Gauge<Double>) Utils.newInstance(this.gauge.getClass().getName()));
                        metric.setMetricName(this.getMetricName());
                        return metric;
                    }
                });

        JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName("GCTime", MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
                    double lastGcTime = getGcTime();

                    @Override
                    public Double getValue() {
                        double newGcTime = getGcTime();
                        double delta = newGcTime - lastGcTime;
                        lastGcTime = newGcTime;
                        return delta;
                    }

                    double getGcTime() {
                        double sum = 0;
                        for (GarbageCollectorMXBean gcBean : gcBeans) {
                            sum += gcBean.getCollectionTime();

                        }
                        // convert time to us
                        return sum * 1000;
                    }
                })
        );

        JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName("GCCount", MetricType.GAUGE),
                new AsmGauge(new Gauge<Double>() {
                    final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
                    double lastGcNum = getGcNum();

                    @Override
                    public Double getValue() {
                        double newGcNum = getGcNum();
                        double delta = newGcNum - lastGcNum;
                        lastGcNum = newGcNum;
                        return delta;
                    }

                    double getGcNum() {
                        double cnt = 0;
                        for (GarbageCollectorMXBean gcBean : gcBeans) {
                            cnt += gcBean.getCollectionCount();
                        }
                        return cnt;
                    }
                })
        );

        LOG.info("Worker Configuration " + stormConf);
        try {
            boolean enableClassloader = ConfigExtension.isEnableTopologyClassLoader(stormConf);
            boolean enableDebugClassloader = ConfigExtension.isEnableClassloaderDebug(stormConf);

            if (jarPath == null && enableClassloader && !conf.get(Config.STORM_CLUSTER_MODE).equals("local")) {
                LOG.error("classloader is enabled, but no app jar was found!");
                throw new InvalidParameterException();
            }

            URL[] urlArray = new URL[0];
            if (jarPath != null) {
                String[] paths = jarPath.split(":");
                Set<URL> urls = new HashSet<>();
                for (String path : paths) {
                    if (StringUtils.isBlank(path))
                        continue;
                    URL url = new URL("File:" + path);
                    urls.add(url);
                }
                urlArray = urls.toArray(new URL[0]);
            }

            WorkerClassLoader.mkInstance(urlArray, ClassLoader.getSystemClassLoader(),
                    ClassLoader.getSystemClassLoader().getParent(), enableClassloader, enableDebugClassloader);
        } catch (Exception e) {
            LOG.error("init jarClassLoader error!", e);
            throw new InvalidParameterException();
        }

        // register kryo serializer for HeapByteBuffer, it's a hack as HeapByteBuffer is non-public
        Config.registerSerialization(stormConf, "java.nio.HeapByteBuffer", KryoByteBufferSerializer.class);

        if (this.context == null) {
            this.context = TransportFactory.makeContext(stormConf);
        }

        // this.transferCtrlQueue = new LinkedBlockingQueue<TransferData>();
        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 256);
        long timeout = JStormUtils.parseLong(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        this.transferCtrlQueue = DisruptorQueue.mkInstance(
                "TotalTransfer", ProducerType.MULTI, queueSize, waitStrategy, false, 0, 0);

        //metric for transferCtrlQueue
        QueueGauge transferCtrlGauge = new QueueGauge(transferCtrlQueue, MetricDef.SEND_QUEUE);
        JStormMetrics.registerWorkerMetric(JStormMetrics.workerMetricName(
                MetricDef.SEND_QUEUE, MetricType.GAUGE), new AsmGauge(transferCtrlGauge));

        this.nodePortToSocket = new ConcurrentHashMap<>();
        this.taskToNodePort = new ConcurrentHashMap<>();
        this.workerToResource = new ConcurrentSkipListSet<>();
        this.innerTaskTransfer = new ConcurrentHashMap<>();
        this.controlQueues = new ConcurrentHashMap<>();
        this.deserializeQueues = new ConcurrentHashMap<>();
        this.tasksToComponent = new ConcurrentHashMap<>();
        this.componentToSortedTasks = new ConcurrentHashMap<>();

        Assignment assignment = zkCluster.assignment_info(this.topologyId, null);
        if (assignment == null) {
            String errMsg = "Failed to get assignment of " + this.topologyId;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        workerToResource.addAll(assignment.getWorkers());

        // get current worker's task list
        this.taskIds = assignment.getCurrentWorkerTasks(this.supervisorId, port);
        if (taskIds.size() == 0) {
            throw new RuntimeException("Current worker doesn't contain any tasks!");
        }
        LOG.info("Current worker taskList:" + taskIds);

        // deserialize topology code from local dir
        rawTopology = StormConfig.read_supervisor_topology_code(conf, topologyId);
        sysTopology = Common.system_topology(stormConf, rawTopology);

        generateMaps();

        contextMaker = new ContextMaker(this);

        outTaskStatus = new ConcurrentHashMap<>();

        int minPoolSize = taskIds.size() > 5 ? 5 : taskIds.size();
        int maxPoolSize = 2 * taskIds.size();

        threadPool = Executors.newScheduledThreadPool(THREAD_POOL_NUM);
        TimerTrigger.setScheduledExecutorService(threadPool);

        if (ConfigExtension.getWorkerFlushPoolMinSize(stormConf) != null) {
            minPoolSize = ConfigExtension.getWorkerFlushPoolMinSize(stormConf);
        }
        if (ConfigExtension.getWorkerFlushPoolMaxSize(stormConf) != null) {
            maxPoolSize = ConfigExtension.getWorkerFlushPoolMaxSize(stormConf);
        }
        if (minPoolSize > maxPoolSize) {
            LOG.error("Irrational flusher pool parameter configuration");
        }
        flusherPool = new FlusherPool(minPoolSize, maxPoolSize, 30, TimeUnit.SECONDS);
        Flusher.setFlusherPool(flusherPool);

        if (!StormConfig.local_mode(stormConf)) {
            healthReporterThread = new AsyncLoopThread(new JStormHealthReporter(this));
        }

        try {
            assignmentTS = StormConfig.read_supervisor_topology_timestamp(conf, topologyId);
        } catch (FileNotFoundException e) {
            assignmentTS = System.currentTimeMillis();
        }

        outboundTasks = new HashSet<>();

        // kryo
        updateKryoSerializer();

        LOG.info("Successfully created WorkerData");
    }

    private void registerUpdateListeners() {
        updateListener = new UpdateListener();

        //disable/enable metrics on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                metricReporter.updateMetricConfig(combineConf(conf));
            }
        });

        //disable/enable topology debug on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                JStormDebugger.update(combineConf(conf));
            }
        });

        //change log level on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                LogUtils.update(combineConf(conf));
            }
        });
    }

    private Map<Object, Object> combineConf(Map newConf) {
        Map<Object, Object> ret = new HashMap<>(this.conf);
        ret.putAll(newConf);
        return ret;
    }

    public UpdateListener getUpdateListener() {
        return updateListener;
    }

    // create kryo serializer
    public void updateKryoSerializer() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        KryoTupleDeserializer kryoTupleDeserializer = new KryoTupleDeserializer(stormConf, workerTopologyContext, workerTopologyContext.getRawTopology());
        KryoTupleSerializer kryoTupleSerializer = new KryoTupleSerializer(stormConf, workerTopologyContext.getRawTopology());

        atomKryoDeserializer.getAndSet(kryoTupleDeserializer);
        atomKryoSerializer.getAndSet(kryoTupleSerializer);
    }


    /**
     * private ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort;
     * private HashMap<Integer, String> tasksToComponent;
     * private Map<String, List<Integer>> componentToSortedTasks;
     * private Map<String, Map<String, Fields>> componentToStreamToFields;
     * private Map<String, Object> defaultResources;
     * private Map<String, Object> userResources;
     * private Map<String, Object> executorData;
     * private Map registeredMetrics;
     *
     * @throws Exception
     */
    private void generateMaps() throws Exception {
        updateTaskComponentMap();
        this.defaultResources = new HashMap<>();
        this.userResources = new HashMap<>();
        this.executorData = new HashMap<>();
        this.registeredMetrics = new HashMap();
    }

    public Map<Object, Object> getRawConf() {
        return conf;
    }

    public AtomicBoolean getShutdown() {
        return shutdown;
    }

    public StatusType getTopologyStatus() {
        return topologyStatus;
    }

    public void setTopologyStatus(StatusType topologyStatus) {
        this.topologyStatus = topologyStatus;
    }

    public Map<Object, Object> getConf() {
        return conf;
    }

    public Map<Object, Object> getStormConf() {
        return stormConf;
    }

    public IContext getContext() {
        return context;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public Integer getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    public ClusterState getZkClusterState() {
        return zkClusterState;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Set<Integer> getTaskIds() {
        return taskIds;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodePortToSocket() {
        return nodePortToSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskToNodePort() {
        return taskToNodePort;
    }

    public void updateWorkerToResource(Set<ResourceWorkerSlot> workers) {
        workerToResource.removeAll(workers);
        workerToResource.addAll(workers);
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getInnerTaskTransfer() {
        return innerTaskTransfer;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getDeserializeQueues() {
        return deserializeQueues;
    }

    public ConcurrentHashMap<Integer, DisruptorQueue> getControlQueues() {
        return controlQueues;
    }

    public ConcurrentHashMap<Integer, String> getTasksToComponent() {
        return tasksToComponent;
    }

    public StormTopology getRawTopology() {
        return rawTopology;
    }

    public StormTopology getSysTopology() {
        return sysTopology;
    }

    public ContextMaker getContextMaker() {
        return contextMaker;
    }

    public AsyncLoopDefaultKill getWorkHalt() {
        return workHalt;
    }

    public DisruptorQueue getTransferCtrlQueue() {
        return transferCtrlQueue;
    }

    // public LinkedBlockingQueue<TransferData> getTransferCtrlQueue() {
    // return transferCtrlQueue;
    // }

    public Map<String, List<Integer>> getComponentToSortedTasks() {
        return componentToSortedTasks;
    }

    public Map<String, Object> getDefaultResources() {
        return defaultResources;
    }

    public Map<String, Object> getUserResources() {
        return userResources;
    }

    public Map<String, Object> getExecutorData() {
        return executorData;
    }

    public Map getRegisteredMetrics() {
        return registeredMetrics;
    }

    public List<TaskShutdownDameon> getShutdownTasks() {
        return shutdownTasks;
    }

    public void setShutdownTasks(List<TaskShutdownDameon> shutdownTasks) {
        this.shutdownTasks = shutdownTasks;
    }

    public void addShutdownTask(TaskShutdownDameon shutdownTask) {
        this.shutdownTasks.add(shutdownTask);
    }

    public List<TaskShutdownDameon> getShutdownDaemonbyTaskIds(Set<Integer> taskIds) {
        List<TaskShutdownDameon> ret = new ArrayList<>();
        for (TaskShutdownDameon shutdown : shutdownTasks) {
            if (taskIds.contains(shutdown.getTaskId()))
                ret.add(shutdown);
        }
        return ret;
    }

    public AtomicBoolean getWorkerInitConnectionStatus() {
        return workerInitConnectionStatus;
    }

    public void initOutboundTaskStatus(Set<Integer> outboundTasks) {
        for (Integer taskId : outboundTasks) {
            outTaskStatus.put(taskId, false);
        }
    }

    public Map<Integer, Boolean> getOutboundTaskStatus() {
        return outTaskStatus;
    }

    public void addOutboundTaskStatusIfAbsent(Integer taskId) {
        outTaskStatus.putIfAbsent(taskId, false);
    }

    public void removeOutboundTaskStatus(Integer taskId) {
        outTaskStatus.remove(taskId);
    }

    public void updateOutboundTaskStatus(Integer taskId, boolean isActive) {
        outTaskStatus.put(taskId, isActive);
    }

    public boolean isOutboundTaskActive(Integer taskId) {
        return outTaskStatus.get(taskId) != null ? outTaskStatus.get(taskId) : false;
    }

    public ScheduledExecutorService getThreadPool() {
        return threadPool;
    }

    public void setAssignmentTs(Long time) {
        assignmentTS = time;
    }

    public Long getAssignmentTs() {
        return assignmentTS;
    }

    public void setAssignmentType(AssignmentType type) {
        this.assignmentType = type;
    }

    public AssignmentType getAssignmentType() {
        return assignmentType;
    }

    public void updateWorkerData(Assignment assignment) throws Exception {
        updateTaskIds(assignment);
        updateTaskComponentMap();
        updateStormTopology();
    }

    public void updateTaskIds(Assignment assignment) {
        this.taskIds.clear();
        this.taskIds.addAll(assignment.getCurrentWorkerTasks(supervisorId, port));
    }

    public Set<Integer> getLocalNodeTasks() {
        return localNodeTasks;
    }

    public void setLocalNodeTasks(Set<Integer> localNodeTasks) {
        this.localNodeTasks = localNodeTasks;
    }

    public void setOutboundTasks(Set<Integer> outboundTasks) {
        this.outboundTasks = outboundTasks;
    }

    public Set<Integer> getOutboundTasks() {
        return outboundTasks;
    }

    private void updateTaskComponentMap() throws Exception {
        Map<Integer, String> tmp = Common.getTaskToComponent(Cluster.get_all_taskInfo(zkCluster, topologyId));

        this.tasksToComponent.putAll(tmp);
        LOG.info("Updated tasksToComponentMap:" + tasksToComponent);

        this.componentToSortedTasks.putAll(JStormUtils.reverse_map(tmp));
        for (java.util.Map.Entry<String, List<Integer>> entry : componentToSortedTasks.entrySet()) {
            List<Integer> tasks = entry.getValue();

            Collections.sort(tasks);
        }
    }

    private void updateStormTopology() {
        StormTopology rawTmp;
        StormTopology sysTmp;
        try {
            rawTmp = StormConfig.read_supervisor_topology_code(conf, topologyId);
            sysTmp = Common.system_topology(stormConf, rawTopology);
        } catch (IOException e) {
            LOG.error("Failed to read supervisor topology code for " + topologyId, e);
            return;
        } catch (InvalidTopologyException e) {
            LOG.error("Failed to update sysTopology for " + topologyId, e);
            return;
        }

        updateTopology(rawTopology, rawTmp);
        updateTopology(sysTopology, sysTmp);
    }

    private void updateTopology(StormTopology oldTopology, StormTopology newTopology) {
        oldTopology.set_bolts(newTopology.get_bolts());
        oldTopology.set_spouts(newTopology.get_spouts());
        oldTopology.set_state_spouts(newTopology.get_state_spouts());
    }

    public AtomicBoolean getMonitorEnable() {
        return monitorEnable;
    }

    public IConnection getRecvConnection() {
        return recvConnection;
    }

    public void setRecvConnection(IConnection recvConnection) {
        this.recvConnection = recvConnection;
    }

    public JStormMetricsReporter getMetricsReporter() {
        return metricReporter;
    }

    public void setMetricsReporter(JStormMetricsReporter metricReporter) {
        this.metricReporter = metricReporter;
    }

    public HashMap<String, Map<String, Fields>> generateComponentToStreamToFields(StormTopology topology) {
        HashMap<String, Map<String, Fields>> componentToStreamToFields = new HashMap<>();

        Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
        for (String component : components) {

            Map<String, Fields> streamToFieldsMap = new HashMap<>();

            Map<String, StreamInfo> streamInfoMap = ThriftTopologyUtils.getComponentCommon(topology, component).get_streams();
            for (Map.Entry<String, StreamInfo> entry : streamInfoMap.entrySet()) {
                String streamId = entry.getKey();
                StreamInfo streamInfo = entry.getValue();

                streamToFieldsMap.put(streamId, new Fields(streamInfo.get_output_fields()));
            }

            componentToStreamToFields.put(component, streamToFieldsMap);
        }
        return componentToStreamToFields;
    }

    public AtomicReference<KryoTupleDeserializer> getAtomKryoDeserializer() {
        return atomKryoDeserializer;
    }

    public AtomicReference<KryoTupleSerializer> getAtomKryoSerializer() {
        return atomKryoSerializer;
    }

    protected List<AsyncLoopThread> setDeserializeThreads() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        int tasksNum = shutdownTasks.size();
        double workerRatio = ConfigExtension.getWorkerDeserializeThreadRatio(stormConf);
        int workerDeserThreadNum = Utils.getInt(Math.ceil(workerRatio * tasksNum));
        if (workerDeserThreadNum > 0 && tasksNum > 0) {
            double average = tasksNum / (double) workerDeserThreadNum;
            for (int i = 0; i < workerDeserThreadNum; i++) {
                int startRunTaskIndex = Utils.getInt(Math.rint(average * i));
                deserializeThreads.add(new AsyncLoopThread(new WorkerDeserializeRunnable(
                        shutdownTasks, stormConf, workerTopologyContext, startRunTaskIndex, i)));
            }
        }
        return deserializeThreads;
    }

    protected List<AsyncLoopThread> setSerializeThreads() {
        WorkerTopologyContext workerTopologyContext = contextMaker.makeWorkerTopologyContext(sysTopology);
        int tasksNum = shutdownTasks.size();
        double workerRatio = ConfigExtension.getWorkerSerializeThreadRatio(stormConf);
        int workerSerialThreadNum = Utils.getInt(Math.ceil(workerRatio * tasksNum));
        if (workerSerialThreadNum > 0 && tasksNum > 0) {
            double average = tasksNum / (double) workerSerialThreadNum;
            for (int i = 0; i < workerSerialThreadNum; i++) {
                int startRunTaskIndex = Utils.getInt(Math.rint(average * i));
                serializeThreads.add(new AsyncLoopThread(new WorkerSerializeRunnable(
                        shutdownTasks, stormConf, workerTopologyContext, startRunTaskIndex, i)));
            }
        }
        return serializeThreads;
    }

    public FlusherPool getFlusherPool() {
        return flusherPool;
    }
}
