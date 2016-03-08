
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
import backtype.storm.task.GeneralTopologyContext;
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
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
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
    private ClusterState zkClusterstate;
    private StormClusterState zkCluster;

    // running taskId list in current worker
    private Set<Integer> taskids;

    // connection to other workers <NodePort, ZMQConnection>
    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    // <taskId, NodePort>
    private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    private ConcurrentSkipListSet<ResourceWorkerSlot> workerToResource;

    private volatile Set<Integer> outboundTasks;
    private Set<Integer> localTasks;
    private Set<Integer> localNodeTasks;


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

    // raw topology is deserialized from local jar
    // it doesn't contain acker
    private StormTopology rawTopology;
    // sys topology is the running topology in the worker
    // it contain ackers
    private StormTopology sysTopology;

    private ContextMaker contextMaker;

    // shutdown woker entrance
    private final AsyncLoopDefaultKill workHalt = new AsyncLoopDefaultKill();

    // sending tuple's queue
    // private LinkedBlockingQueue<TransferData> transferCtrlQueue;
    private DisruptorQueue transferCtrlQueue;

    private DisruptorQueue sendingQueue;

    private List<TaskShutdownDameon> shutdownTasks;

    private ConcurrentHashMap<Integer, Boolean> outTaskStatus; // true => active

    public static final int THREAD_POOL_NUM = 4;
    private ScheduledExecutorService threadPool;

    private volatile Long assignmentTS; // Assignment timeStamp. The time of
    // last update of assignment

    private volatile AssignmentType assignmentType;

    private IConnection recvConnection;

    private JStormMetricsReporter metricReporter;

    private AsyncLoopThread healthReporterThread;

    private AtomicBoolean workerOldStatus;

    private AtomicBoolean workeInitConnectionStatus;
    
    private AtomicReference<KryoTupleSerializer> atomKryoSerializer = new AtomicReference<>();

    private AtomicReference<KryoTupleDeserializer> atomKryoDeserializer = new AtomicReference<>();

    // the data/config which need dynamic update
    private UpdateListener updateListener;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public WorkerData(Map conf, IContext context, String topology_id, String supervisor_id, int port, String worker_id, String jar_path) throws Exception {

        this.conf = conf;
        this.context = context;
        this.topologyId = topology_id;
        this.supervisorId = supervisor_id;
        this.port = port;
        this.workerId = worker_id;

        this.shutdown = new AtomicBoolean(false);

        this.monitorEnable = new AtomicBoolean(true);
        this.topologyStatus = StatusType.active;

        this.workerOldStatus = new AtomicBoolean(false);
        this.workeInitConnectionStatus = new AtomicBoolean(false);

        if (StormConfig.cluster_mode(conf).equals("distributed")) {
            String pidDir = StormConfig.worker_pids_root(conf, worker_id);
            JStormServerUtils.createPid(pidDir);
        }

        // create zk interface
        this.zkClusterstate = ZkTool.mk_distributed_cluster_state(conf);
        this.zkCluster = Cluster.mk_storm_cluster_state(zkClusterstate);

        Map rawConf = StormConfig.read_supervisor_topology_conf(conf, topology_id);
        this.stormConf = new HashMap<Object, Object>();
        this.stormConf.putAll(conf);
        this.stormConf.putAll(rawConf);

        // init topology.debug and other debug args
        JStormDebugger.update(stormConf);
        // register dynamic updaters
        registerUpdateListeners();

        JStormMetrics.setTopologyId(topology_id);
        JStormMetrics.setPort(port);
        JStormMetrics.setDebug(ConfigExtension.isEnableMetricDebug(stormConf));
        JStormMetrics.enabled = ConfigExtension.isEnableMetrics(stormConf);
        JStormMetrics.enableStreamMetrics = ConfigExtension.isEnableStreamMetrics(stormConf);
        JStormMetrics.addDebugMetrics(ConfigExtension.getDebugMetricNames(stormConf));
        AsmMetric.setSampleRate(ConfigExtension.getMetricSampleRate(stormConf));

        ConfigExtension.setLocalSupervisorId(stormConf, supervisorId);
        ConfigExtension.setLocalWorkerId(stormConf, workerId);
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
        
        

        /*
        JStormMetrics.registerWorkerMetric(JStormMetrics.workerMetricName(MetricDef.DISK_USAGE, MetricType.GAUGE), new AsmGauge(new Gauge<Double>() {
            @Override
            public Double getValue() {
                return JStormUtils.getDiskUsage();
            }
        }));
        */

        LOG.info("Worker Configuration " + stormConf);

        try {
            boolean enableClassloader = ConfigExtension.isEnableTopologyClassLoader(stormConf);
            boolean enableDebugClassloader = ConfigExtension.isEnableClassloaderDebug(stormConf);

            if (jar_path == null && enableClassloader == true && !conf.get(Config.STORM_CLUSTER_MODE).equals("local")) {
                LOG.error("enable classloader, but not app jar");
                throw new InvalidParameterException();
            }

            URL[] urlArray = new URL[0];
            if (jar_path != null) {
                String[] paths = jar_path.split(":");
                Set<URL> urls = new HashSet<URL>();
                for (String path : paths) {
                    if (StringUtils.isBlank(path))
                        continue;
                    URL url = new URL("File:" + path);
                    urls.add(url);
                }
                urlArray = urls.toArray(new URL[0]);
            }

            WorkerClassLoader.mkInstance(urlArray, ClassLoader.getSystemClassLoader(), ClassLoader.getSystemClassLoader().getParent(), enableClassloader,
                    enableDebugClassloader);
        } catch (Exception e) {
            LOG.error("init jarClassLoader error!", e);
            throw new InvalidParameterException();
        }

        // register kryo serializer for HeapByteBuffer, it's a hack as HeapByteBuffer is non-public
        Config.registerSerialization(stormConf, "java.nio.HeapByteBuffer", KryoByteBufferSerializer.class);

        if (this.context == null) {
            this.context = TransportFactory.makeContext(stormConf);
        }

        boolean disruptorUseSleep = ConfigExtension.isDisruptorUseSleep(stormConf);
        DisruptorQueue.setUseSleep(disruptorUseSleep);
        boolean isLimited = ConfigExtension.getTopologyBufferSizeLimited(stormConf);
        DisruptorQueue.setLimited(isLimited);
        LOG.info("Disruptor use sleep:" + disruptorUseSleep + ", limited size:" + isLimited);

        // this.transferCtrlQueue = new LinkedBlockingQueue<TransferData>();
        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 256);
        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(stormConf);
        this.transferCtrlQueue = DisruptorQueue.mkInstance("TotalTransfer", ProducerType.MULTI, queueSize, waitStrategy);

        //metric for transferCtrlQueue
        QueueGauge transferCtrlGauge = new QueueGauge(transferCtrlQueue, MetricDef.SEND_QUEUE);
        JStormMetrics.registerWorkerMetric(JStormMetrics.workerMetricName(MetricDef.SEND_QUEUE, MetricType.GAUGE), new AsmGauge(
                transferCtrlGauge));

        //this.transferCtrlQueue.consumerStarted();
        int buffer_size = Utils.getInt(stormConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        this.sendingQueue = DisruptorQueue.mkInstance("TotalSending", ProducerType.MULTI, buffer_size, waitStrategy);
        //this.sendingQueue.consumerStarted();

        this.nodeportSocket = new ConcurrentHashMap<WorkerSlot, IConnection>();
        this.taskNodeport = new ConcurrentHashMap<Integer, WorkerSlot>();
        this.workerToResource = new ConcurrentSkipListSet<ResourceWorkerSlot>();
        this.innerTaskTransfer = new ConcurrentHashMap<Integer, DisruptorQueue>();
        this.controlQueues = new ConcurrentHashMap<Integer, DisruptorQueue>();
        this.deserializeQueues = new ConcurrentHashMap<Integer, DisruptorQueue>();
        this.tasksToComponent = new ConcurrentHashMap<Integer, String>();
        this.componentToSortedTasks = new ConcurrentHashMap<String, List<Integer>>();

        Assignment assignment = zkCluster.assignment_info(topologyId, null);
        if (assignment == null) {
            String errMsg = "Failed to get Assignment of " + topologyId;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        workerToResource.addAll(assignment.getWorkers());

        // get current worker's task list

        this.taskids = assignment.getCurrentWorkerTasks(supervisorId, port);
        if (taskids.size() == 0) {
            throw new RuntimeException("No tasks running current workers");
        }
        LOG.info("Current worker taskList:" + taskids);

        // deserialize topology code from local dir
        rawTopology = StormConfig.read_supervisor_topology_code(conf, topology_id);
        sysTopology = Common.system_topology(stormConf, rawTopology);

        generateMaps();

        contextMaker = new ContextMaker(this);

        outTaskStatus = new ConcurrentHashMap<Integer, Boolean>();

        threadPool = Executors.newScheduledThreadPool(THREAD_POOL_NUM);
        TimerTrigger.setScheduledExecutorService(threadPool);

        if (!StormConfig.local_mode(stormConf)) {
            healthReporterThread = new AsyncLoopThread(new JStormHealthReporter(this));
        }

        try {
            Long tmp = StormConfig.read_supervisor_topology_timestamp(conf, topology_id);
            assignmentTS = (tmp == null ? System.currentTimeMillis() : tmp);
        } catch (FileNotFoundException e) {
            assignmentTS = System.currentTimeMillis();
        }

        outboundTasks = new HashSet<Integer>();
        
        // kryo
        updateKryoSerializer();

        LOG.info("Successfully create WorkerData");

    }

    private void registerUpdateListeners(){
        updateListener = new UpdateListener();

        //disable/enable metrics on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                metricReporter.updateMetricConfig(conf);
            }
        });

        //disable/enable topology debug on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                JStormDebugger.update(conf);
            }
        });

        //change log level on the fly
        updateListener.registerUpdater(new UpdateListener.IUpdater() {
            @Override
            public void update(Map conf) {
                LogUtils.update(conf);
            }
        });
    }

    public UpdateListener getUpdateListener(){
        return updateListener;
    }

    // create kryo serilizer
    public void updateKryoSerializer() {
        HashMap<String, Map<String, Fields>> componentToStreamToFields = generatecomponentToStreamToFields(sysTopology);
        GeneralTopologyContext topologyContext =
                new GeneralTopologyContext(sysTopology, stormConf, tasksToComponent, componentToSortedTasks, componentToStreamToFields, topologyId);

        KryoTupleDeserializer kryoTupleDeserializer = new KryoTupleDeserializer(stormConf, topologyContext);
        KryoTupleSerializer kryoTupleSerializer = new KryoTupleSerializer(stormConf, topologyContext);

        atomKryoDeserializer.getAndSet(kryoTupleDeserializer);
        atomKryoSerializer.getAndSet(kryoTupleSerializer);
    }


    /**
     * private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport; private HashMap<Integer, String> tasksToComponent; private Map<String, List<Integer>>
     * componentToSortedTasks; private Map<String, Map<String, Fields>> componentToStreamToFields; private Map<String, Object> defaultResources; private
     * Map<String, Object> userResources; private Map<String, Object> executorData; private Map registeredMetrics;
     *
     * @throws Exception
     */
    private void generateMaps() throws Exception {
        updateTaskComponentMap();

        this.defaultResources = new HashMap<String, Object>();
        this.userResources = new HashMap<String, Object>();
        this.executorData = new HashMap<String, Object>();
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
        return stormConf;
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

    public ClusterState getZkClusterstate() {
        return zkClusterstate;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Set<Integer> getTaskids() {
        return taskids;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodeportSocket() {
        return nodeportSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskNodeport() {
        return taskNodeport;
    }

    public ConcurrentSkipListSet<ResourceWorkerSlot> getWorkerToResource() {
        synchronized (workerToResource) {
            return workerToResource;
        }
    }

    public void updateWorkerToResource(Set<ResourceWorkerSlot> workers) {
        synchronized (workerToResource) {
            Set<ResourceWorkerSlot> oldWorkers = workerToResource.clone();
            oldWorkers.removeAll(workers);
            if (oldWorkers.size() > 0)
                workerToResource.removeAll(workers);
            workerToResource.addAll(workers);
        }
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

    public DisruptorQueue getSendingQueue() {
        return sendingQueue;
    }

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
        List<TaskShutdownDameon> ret = new ArrayList<TaskShutdownDameon>();
        for (TaskShutdownDameon shutdown : shutdownTasks) {
            if (taskIds.contains(shutdown.getTaskId()))
                ret.add(shutdown);
        }
        return ret;
    }

    public AtomicBoolean getWorkerOldStatus() {
        return workerOldStatus;
    }

    public AtomicBoolean getWorkeInitConnectionStatus() {
        return workeInitConnectionStatus;
    }

    public Set<Integer> getLocalTasks() {
        return localTasks;
    }

    public void setLocalTasks(Set<Integer> localTasks) {
        this.localTasks = localTasks;
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
        this.taskids.clear();
        this.taskids.addAll(assignment.getCurrentWorkerTasks(supervisorId, port));
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
    
    public HashMap<String, Map<String, Fields>> generatecomponentToStreamToFields(StormTopology topology){
        HashMap<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();

        Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
        for (String component : components) {

            Map<String, Fields> streamToFieldsMap = new HashMap<String, Fields>();

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
}
