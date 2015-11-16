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
package com.alibaba.jstorm.client;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringUtils;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfigExtension {
    /**
     * if this configure has been set, the spout or bolt will log all receive tuples
     * <p/>
     * topology.debug just for logging all sent tuples
     */
    protected static final String TOPOLOGY_DEBUG_RECV_TUPLE = "topology.debug.recv.tuple";

    public static void setTopologyDebugRecvTuple(Map conf, boolean debug) {
        conf.put(TOPOLOGY_DEBUG_RECV_TUPLE, Boolean.valueOf(debug));
    }

    public static Boolean isTopologyDebugRecvTuple(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_DEBUG_RECV_TUPLE), false);
    }

    private static final String TOPOLOGY_ENABLE_METRIC_DEBUG = "topology.enable.metric.debug";

    public static boolean isEnableMetricDebug(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_METRIC_DEBUG), false);
    }

    private static final String TOPOLOGY_DEBUG_METRIC_NAMES = "topology.debug.metric.names";

    public static String getDebugMetricNames(Map conf) {
        String metrics = (String) conf.get(TOPOLOGY_DEBUG_METRIC_NAMES);
        if (metrics == null) {
            return "";
        }
        return metrics;
    }

    /**
     * metrics switch, ONLY for performance test, DO NOT set it to false in production
     */
    private static final String TOPOLOGY_ENABLE_METRICS = "topology.enable.metrics";

    public static boolean isEnableMetrics(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_METRICS), true);
    }

    /**
     * port number of deamon httpserver server
     */
    private static final Integer DEFAULT_DEAMON_HTTPSERVER_PORT = 7621;

    protected static final String SUPERVISOR_DEAMON_HTTPSERVER_PORT = "supervisor.deamon.logview.port";

    public static Integer getSupervisorDeamonHttpserverPort(Map conf) {
        return JStormUtils.parseInt(conf.get(SUPERVISOR_DEAMON_HTTPSERVER_PORT), DEFAULT_DEAMON_HTTPSERVER_PORT + 1);
    }

    protected static final String NIMBUS_DEAMON_HTTPSERVER_PORT = "nimbus.deamon.logview.port";

    public static Integer getNimbusDeamonHttpserverPort(Map conf) {
        return JStormUtils.parseInt(conf.get(NIMBUS_DEAMON_HTTPSERVER_PORT), DEFAULT_DEAMON_HTTPSERVER_PORT);
    }

    /**
     * Worker gc parameter
     */
    protected static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";

    public static void setWorkerGc(Map conf, String gc) {
        conf.put(WORKER_GC_CHILDOPTS, gc);
    }

    public static String getWorkerGc(Map conf) {
        return (String) conf.get(WORKER_GC_CHILDOPTS);
    }

    protected static final String WOREKER_REDIRECT_OUTPUT = "worker.redirect.output";

    public static boolean getWorkerRedirectOutput(Map conf) {
        Object result = conf.get(WOREKER_REDIRECT_OUTPUT);
        if (result == null)
            return true;
        return (Boolean) result;
    }

    protected static final String WOREKER_REDIRECT_OUTPUT_FILE = "worker.redirect.output.file";

    public static void setWorkerRedirectOutputFile(Map conf, String outputPath) {
        conf.put(WOREKER_REDIRECT_OUTPUT_FILE, outputPath);
    }

    public static String getWorkerRedirectOutputFile(Map conf) {
        return (String) conf.get(WOREKER_REDIRECT_OUTPUT_FILE);
    }

    /**
     * Usually, spout finish prepare before bolt, so spout need wait several seconds so that bolt finish preparation
     * <p/>
     * By default, the setting is 30 seconds
     */
    protected static final String SPOUT_DELAY_RUN = "spout.delay.run";

    public static void setSpoutDelayRunSeconds(Map conf, int delay) {
        conf.put(SPOUT_DELAY_RUN, Integer.valueOf(delay));
    }

    public static int getSpoutDelayRunSeconds(Map conf) {
        return JStormUtils.parseInt(conf.get(SPOUT_DELAY_RUN), 30);
    }

    /**
     * Default ZMQ Pending queue size
     */
    public static final int DEFAULT_ZMQ_MAX_QUEUE_MSG = 1000;

    /**
     * One task will alloc how many memory slot, the default setting is 1
     */
    protected static final String MEM_SLOTS_PER_TASK = "memory.slots.per.task";

    @Deprecated
    public static void setMemSlotPerTask(Map conf, int slotNum) {
        if (slotNum < 1) {
            throw new InvalidParameterException();
        }
        conf.put(MEM_SLOTS_PER_TASK, Integer.valueOf(slotNum));
    }

    /**
     * One task will use cpu slot number, the default setting is 1
     */
    protected static final String CPU_SLOTS_PER_TASK = "cpu.slots.per.task";

    @Deprecated
    public static void setCpuSlotsPerTask(Map conf, int slotNum) {
        if (slotNum < 1) {
            throw new InvalidParameterException();
        }
        conf.put(CPU_SLOTS_PER_TASK, Integer.valueOf(slotNum));
    }

    /**
     * if the setting has been set, the component's task must run different node This is conflict with USE_SINGLE_NODE
     */
    protected static final String TASK_ON_DIFFERENT_NODE = "task.on.differ.node";

    public static void setTaskOnDifferentNode(Map conf, boolean isIsolate) {
        conf.put(TASK_ON_DIFFERENT_NODE, Boolean.valueOf(isIsolate));
    }

    public static boolean isTaskOnDifferentNode(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TASK_ON_DIFFERENT_NODE), false);
    }

    protected static final String SUPERVISOR_ENABLE_CGROUP = "supervisor.enable.cgroup";

    public static boolean isEnableCgroup(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_ENABLE_CGROUP), false);
    }

    /**
     * If component or topology configuration set "use.old.assignment", will try use old assignment firstly
     */
    protected static final String USE_OLD_ASSIGNMENT = "use.old.assignment";

    public static void setUseOldAssignment(Map conf, boolean useOld) {
        conf.put(USE_OLD_ASSIGNMENT, Boolean.valueOf(useOld));
    }

    public static boolean isUseOldAssignment(Map conf) {
        return JStormUtils.parseBoolean(conf.get(USE_OLD_ASSIGNMENT), false);
    }

    /**
     * The supervisor's hostname
     */
    protected static final String SUPERVISOR_HOSTNAME = "supervisor.hostname";
    public static final Object SUPERVISOR_HOSTNAME_SCHEMA = String.class;

    public static String getSupervisorHost(Map conf) {
        return (String) conf.get(SUPERVISOR_HOSTNAME);
    }

    protected static final String SUPERVISOR_USE_IP = "supervisor.use.ip";

    public static boolean isSupervisorUseIp(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_USE_IP), false);
    }

    protected static final String NIMBUS_USE_IP = "nimbus.use.ip";

    public static boolean isNimbusUseIp(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NIMBUS_USE_IP), false);
    }

    protected static final String TOPOLOGY_ENABLE_CLASSLOADER = "topology.enable.classloader";

    public static boolean isEnableTopologyClassLoader(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_CLASSLOADER), false);
    }

    public static void setEnableTopologyClassLoader(Map conf, boolean enable) {
        conf.put(TOPOLOGY_ENABLE_CLASSLOADER, Boolean.valueOf(enable));
    }

    protected static String CLASSLOADER_DEBUG = "classloader.debug";

    public static boolean isEnableClassloaderDebug(Map conf) {
        return JStormUtils.parseBoolean(conf.get(CLASSLOADER_DEBUG), false);
    }

    public static void setEnableClassloaderDebug(Map conf, boolean enable) {
        conf.put(CLASSLOADER_DEBUG, enable);
    }

    protected static final String CONTAINER_NIMBUS_HEARTBEAT = "container.nimbus.heartbeat";

    /**
     * Get to know whether nimbus is run under Apsara/Yarn container
     */
    public static boolean isEnableContainerNimbus() {
        String path = System.getenv(CONTAINER_NIMBUS_HEARTBEAT);

        if (StringUtils.isBlank(path)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get Apsara/Yarn nimbus container's hearbeat dir
     */
    public static String getContainerNimbusHearbeat() {
        return System.getenv(CONTAINER_NIMBUS_HEARTBEAT);
    }

    protected static final String CONTAINER_SUPERVISOR_HEARTBEAT = "container.supervisor.heartbeat";

    /**
     * Get to know whether supervisor is run under Apsara/Yarn supervisor container
     */
    public static boolean isEnableContainerSupervisor() {
        String path = System.getenv(CONTAINER_SUPERVISOR_HEARTBEAT);

        if (StringUtils.isBlank(path)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get Apsara/Yarn supervisor container's hearbeat dir
     */
    public static String getContainerSupervisorHearbeat() {
        return (String) System.getenv(CONTAINER_SUPERVISOR_HEARTBEAT);
    }

    protected static final String CONTAINER_HEARTBEAT_TIMEOUT_SECONDS = "container.heartbeat.timeout.seconds";

    public static int getContainerHeartbeatTimeoutSeconds(Map conf) {
        return JStormUtils.parseInt(conf.get(CONTAINER_HEARTBEAT_TIMEOUT_SECONDS), 240);
    }

    protected static final String CONTAINER_HEARTBEAT_FREQUENCE = "container.heartbeat.frequence";

    public static int getContainerHeartbeatFrequence(Map conf) {
        return JStormUtils.parseInt(conf.get(CONTAINER_HEARTBEAT_FREQUENCE), 10);
    }

    protected static final String JAVA_SANDBOX_ENABLE = "java.sandbox.enable";

    public static boolean isJavaSandBoxEnable(Map conf) {
        return JStormUtils.parseBoolean(conf.get(JAVA_SANDBOX_ENABLE), false);
    }

    protected static String SPOUT_SINGLE_THREAD = "spout.single.thread";

    public static boolean isSpoutSingleThread(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SPOUT_SINGLE_THREAD), false);
    }

    public static void setSpoutSingleThread(Map conf, boolean enable) {
        conf.put(SPOUT_SINGLE_THREAD, enable);
    }

    protected static String WORKER_STOP_WITHOUT_SUPERVISOR = "worker.stop.without.supervisor";

    public static boolean isWorkerStopWithoutSupervisor(Map conf) {
        return JStormUtils.parseBoolean(conf.get(WORKER_STOP_WITHOUT_SUPERVISOR), false);
    }

    protected static String CGROUP_ROOT_DIR = "supervisor.cgroup.rootdir";

    public static String getCgroupRootDir(Map conf) {
        return (String) conf.get(CGROUP_ROOT_DIR);
    }

    protected static String NETTY_TRANSFER_ASYNC_AND_BATCH = "storm.messaging.netty.transfer.async.batch";

    public static boolean isNettyTransferAsyncBatch(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NETTY_TRANSFER_ASYNC_AND_BATCH), true);
    }

    protected static final String USE_USERDEFINE_ASSIGNMENT = "use.userdefine.assignment";

    public static void setUserDefineAssignment(Map conf, List<WorkerAssignment> userDefines) {
        List<String> ret = new ArrayList<String>();
        for (WorkerAssignment worker : userDefines) {
            ret.add(Utils.to_json(worker));
        }
        conf.put(USE_USERDEFINE_ASSIGNMENT, ret);
    }

    public static List<WorkerAssignment> getUserDefineAssignment(Map conf) {
        List<WorkerAssignment> ret = new ArrayList<WorkerAssignment>();
        if (conf.get(USE_USERDEFINE_ASSIGNMENT) == null)
            return ret;
        for (String worker : (List<String>) conf.get(USE_USERDEFINE_ASSIGNMENT)) {
            ret.add(WorkerAssignment.parseFromObj(Utils.from_json(worker)));
        }
        return ret;
    }

    protected static String NETTY_PENDING_BUFFER_TIMEOUT = "storm.messaging.netty.pending.buffer.timeout";

    public static void setNettyPendingBufferTimeout(Map conf, Long timeout) {
        conf.put(NETTY_PENDING_BUFFER_TIMEOUT, timeout);
    }

    public static long getNettyPendingBufferTimeout(Map conf) {
        int messageTimeout = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 120);
        return JStormUtils.parseLong(conf.get(NETTY_PENDING_BUFFER_TIMEOUT), messageTimeout * 1000);
    }

    protected static final String MEMSIZE_PER_WORKER = "worker.memory.size";

    public static void setMemSizePerWorker(Map conf, long memSize) {
        conf.put(MEMSIZE_PER_WORKER, memSize);
    }

    public static void setMemSizePerWorkerByKB(Map conf, long memSize) {
        long size = memSize * 1024l;
        setMemSizePerWorker(conf, size);
    }

    public static void setMemSizePerWorkerByMB(Map conf, long memSize) {
        long size = memSize * 1024l;
        setMemSizePerWorkerByKB(conf, size);
    }

    public static void setMemSizePerWorkerByGB(Map conf, long memSize) {
        long size = memSize * 1024l;
        setMemSizePerWorkerByMB(conf, size);
    }

    public static long getMemSizePerWorker(Map conf) {
        long size = JStormUtils.parseLong(conf.get(MEMSIZE_PER_WORKER), JStormUtils.SIZE_1_G * 2);
        return size > 0 ? size : JStormUtils.SIZE_1_G * 2;
    }

    protected static final String MIN_MEMSIZE_PER_WORKER = "worker.memory.min.size";

    public static void setMemMinSizePerWorker(Map conf, long memSize) {
        conf.put(MIN_MEMSIZE_PER_WORKER, memSize);
    }

    public static long getMemMinSizePerWorker(Map conf) {
        long maxMemSize = getMemSizePerWorker(conf);

        Long size = JStormUtils.parseLong(conf.get(MIN_MEMSIZE_PER_WORKER));
        long minMemSize = (size == null || size == 0) ? maxMemSize : size;

        return minMemSize;
    }

    protected static final String CPU_SLOT_PER_WORKER = "worker.cpu.slot.num";

    public static void setCpuSlotNumPerWorker(Map conf, int slotNum) {
        conf.put(CPU_SLOT_PER_WORKER, slotNum);
    }

    public static int getCpuSlotPerWorker(Map conf) {
        int slot = JStormUtils.parseInt(conf.get(CPU_SLOT_PER_WORKER), 1);
        return slot > 0 ? slot : 1;
    }

    protected static String TOPOLOGY_PERFORMANCE_METRICS = "topology.performance.metrics";

    public static boolean isEnablePerformanceMetrics(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_PERFORMANCE_METRICS), true);
    }

    public static void setPerformanceMetrics(Map conf, boolean isEnable) {
        conf.put(TOPOLOGY_PERFORMANCE_METRICS, isEnable);
    }

    protected static String NETTY_BUFFER_THRESHOLD_SIZE = "storm.messaging.netty.buffer.threshold";

    public static long getNettyBufferThresholdSize(Map conf) {
        return JStormUtils.parseLong(conf.get(NETTY_BUFFER_THRESHOLD_SIZE), 8 * JStormUtils.SIZE_1_M);
    }

    public static void setNettyBufferThresholdSize(Map conf, long size) {
        conf.put(NETTY_BUFFER_THRESHOLD_SIZE, size);
    }

    protected static String NETTY_MAX_SEND_PENDING = "storm.messaging.netty.max.pending";

    public static void setNettyMaxSendPending(Map conf, long pending) {
        conf.put(NETTY_MAX_SEND_PENDING, pending);
    }

    public static long getNettyMaxSendPending(Map conf) {
        return JStormUtils.parseLong(conf.get(NETTY_MAX_SEND_PENDING), 16);
    }

    protected static String DISRUPTOR_USE_SLEEP = "disruptor.use.sleep";

    public static boolean isDisruptorUseSleep(Map conf) {
        return JStormUtils.parseBoolean(conf.get(DISRUPTOR_USE_SLEEP), true);
    }

    public static void setDisruptorUseSleep(Map conf, boolean useSleep) {
        conf.put(DISRUPTOR_USE_SLEEP, useSleep);
    }

    public static boolean isTopologyContainAcker(Map conf) {
        int num = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
        if (num > 0) {
            return true;
        } else {
            return false;
        }
    }

    protected static String NETTY_SYNC_MODE = "storm.messaging.netty.sync.mode";

    public static boolean isNettySyncMode(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NETTY_SYNC_MODE), false);
    }

    public static void setNettySyncMode(Map conf, boolean sync) {
        conf.put(NETTY_SYNC_MODE, sync);
    }

    protected static String NETTY_ASYNC_BLOCK = "storm.messaging.netty.async.block";

    public static boolean isNettyASyncBlock(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NETTY_ASYNC_BLOCK), true);
    }

    public static void setNettyASyncBlock(Map conf, boolean block) {
        conf.put(NETTY_ASYNC_BLOCK, block);
    }

    protected static String ALIMONITOR_METRICS_POST = "topology.alimonitor.metrics.post";

    public static boolean isAlimonitorMetricsPost(Map conf) {
        return JStormUtils.parseBoolean(conf.get(ALIMONITOR_METRICS_POST), true);
    }

    public static void setAlimonitorMetricsPost(Map conf, boolean post) {
        conf.put(ALIMONITOR_METRICS_POST, post);
    }

    public static String TASK_CLEANUP_TIMEOUT_SEC = "task.cleanup.timeout.sec";

    public static int getTaskCleanupTimeoutSec(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_CLEANUP_TIMEOUT_SEC), 10);
    }

    public static void setTaskCleanupTimeoutSec(Map conf, int timeout) {
        conf.put(TASK_CLEANUP_TIMEOUT_SEC, timeout);
    }

    protected static String UI_CLUSTERS = "ui.clusters";
    protected static String UI_CLUSTER_NAME = "name";
    protected static String UI_CLUSTER_ZK_ROOT = "zkRoot";
    protected static String UI_CLUSTER_ZK_SERVERS = "zkServers";
    protected static String UI_CLUSTER_ZK_PORT = "zkPort";

    public static List<Map> getUiClusters(Map conf) {
        return (List<Map>) conf.get(UI_CLUSTERS);
    }

    public static void setUiClusters(Map conf, List<Map> uiClusters) {
        conf.put(UI_CLUSTERS, uiClusters);
    }

    public static Map getUiClusterInfo(List<Map> uiClusters, String name) {
        Map ret = null;
        for (Map cluster : uiClusters) {
            String clusterName = getUiClusterName(cluster);
            if (clusterName.equals(name)) {
                ret = cluster;
                break;
            }
        }

        return ret;
    }

    public static String getUiClusterName(Map uiCluster) {
        return (String) uiCluster.get(UI_CLUSTER_NAME);
    }

    public static String getUiClusterZkRoot(Map uiCluster) {
        return (String) uiCluster.get(UI_CLUSTER_ZK_ROOT);
    }

    public static List<String> getUiClusterZkServers(Map uiCluster) {
        return (List<String>) uiCluster.get(UI_CLUSTER_ZK_SERVERS);
    }

    public static Integer getUiClusterZkPort(Map uiCluster) {
        return JStormUtils.parseInt(uiCluster.get(UI_CLUSTER_ZK_PORT));
    }


    protected static String SPOUT_PEND_FULL_SLEEP = "spout.pending.full.sleep";

    public static boolean isSpoutPendFullSleep(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SPOUT_PEND_FULL_SLEEP), false);
    }

    public static void setSpoutPendFullSleep(Map conf, boolean sleep) {
        conf.put(SPOUT_PEND_FULL_SLEEP, sleep);

    }

    protected static String LOGVIEW_ENCODING = "supervisor.deamon.logview.encoding";
    protected static String UTF8 = "utf-8";

    public static String getLogViewEncoding(Map conf) {
        String ret = (String) conf.get(LOGVIEW_ENCODING);
        if (ret == null)
            ret = UTF8;
        return ret;
    }

    public static void setLogViewEncoding(Map conf, String enc) {
        conf.put(LOGVIEW_ENCODING, enc);
    }

    protected static String LOG_PAGE_SIZE = "log.page.size";

    public static int getLogPageSize(Map conf) {
        return JStormUtils.parseInt(conf.get(LOG_PAGE_SIZE), 32 * 1024);
    }

    public static void setLogPageSize(Map conf, int pageSize) {
        conf.put(LOG_PAGE_SIZE, pageSize);
    }

    public static String TASK_STATUS_ACTIVE = "Active";
    public static String TASK_STATUS_INACTIVE = "Inactive";
    public static String TASK_STATUS_STARTING = "Starting";

    protected static String ALIMONITOR_TOPO_METIRC_NAME = "topology.alimonitor.topo.metrics.name";
    protected static String ALIMONITOR_TASK_METIRC_NAME = "topology.alimonitor.task.metrics.name";
    protected static String ALIMONITOR_WORKER_METIRC_NAME = "topology.alimonitor.worker.metrics.name";
    protected static String ALIMONITOR_USER_METIRC_NAME = "topology.alimonitor.user.metrics.name";

    public static String getAlmonTopoMetricName(Map conf) {
        return (String) conf.get(ALIMONITOR_TOPO_METIRC_NAME);
    }

    public static String getAlmonTaskMetricName(Map conf) {
        return (String) conf.get(ALIMONITOR_TASK_METIRC_NAME);
    }

    public static String getAlmonWorkerMetricName(Map conf) {
        return (String) conf.get(ALIMONITOR_WORKER_METIRC_NAME);
    }

    public static String getAlmonUserMetricName(Map conf) {
        return (String) conf.get(ALIMONITOR_USER_METIRC_NAME);
    }

    protected static String SPOUT_PARALLELISM = "topology.spout.parallelism";
    protected static String BOLT_PARALLELISM = "topology.bolt.parallelism";

    public static Integer getSpoutParallelism(Map conf, String componentName) {
        Integer ret = null;
        Map<String, String> map = (Map<String, String>) (conf.get(SPOUT_PARALLELISM));
        if (map != null)
            ret = JStormUtils.parseInt(map.get(componentName));
        return ret;
    }

    public static Integer getBoltParallelism(Map conf, String componentName) {
        Integer ret = null;
        Map<String, String> map = (Map<String, String>) (conf.get(BOLT_PARALLELISM));
        if (map != null)
            ret = JStormUtils.parseInt(map.get(componentName));
        return ret;
    }

    protected static String TOPOLOGY_BUFFER_SIZE_LIMITED = "topology.buffer.size.limited";

    public static void setTopologyBufferSizeLimited(Map conf, boolean limited) {
        conf.put(TOPOLOGY_BUFFER_SIZE_LIMITED, limited);
    }

    public static boolean getTopologyBufferSizeLimited(Map conf) {
        boolean isSynchronized = isNettySyncMode(conf);
        if (isSynchronized == true) {
            return true;
        }

        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_BUFFER_SIZE_LIMITED), true);

    }

    protected static String SUPERVISOR_SLOTS_PORTS_BASE = "supervisor.slots.ports.base";

    public static int getSupervisorSlotsPortsBase(Map conf) {
        return JStormUtils.parseInt(conf.get(SUPERVISOR_SLOTS_PORTS_BASE), 6800);
    }

    // SUPERVISOR_SLOTS_PORTS_BASE don't provide setting function, it must be
    // set by configuration

    protected static String SUPERVISOR_SLOTS_PORT_CPU_WEIGHT = "supervisor.slots.port.cpu.weight";

    public static double getSupervisorSlotsPortCpuWeight(Map conf) {
        Object value = conf.get(SUPERVISOR_SLOTS_PORT_CPU_WEIGHT);
        Double ret = JStormUtils.convertToDouble(value);
        if (ret == null || ret <= 0) {
            return 1.2;
        } else {
            return ret;
        }
    }
    
    protected static String SUPERVISOR_SLOTS_PORT_MEM_WEIGHT = "supervisor.slots.port.mem.weight";

    public static double getSupervisorSlotsPortMemWeight(Map conf) {
        Object value = conf.get(SUPERVISOR_SLOTS_PORT_MEM_WEIGHT);
        Double ret = JStormUtils.convertToDouble(value);
        if (ret == null || ret <= 0) {
            return 0.7;
        } else {
            return ret;
        }
    }

    // SUPERVISOR_SLOTS_PORT_CPU_WEIGHT don't provide setting function, it must
    // be set by configuration

    protected static String USER_DEFINED_LOG4J_CONF = "user.defined.log4j.conf";

    public static String getUserDefinedLog4jConf(Map conf) {
        return (String) conf.get(USER_DEFINED_LOG4J_CONF);
    }

    public static void setUserDefinedLog4jConf(Map conf, String fileName) {
        conf.put(USER_DEFINED_LOG4J_CONF, fileName);
    }

    protected static String USER_DEFINED_LOGBACK_CONF = "user.defined.logback.conf";

    public static String getUserDefinedLogbackConf(Map conf) {
        return (String) conf.get(USER_DEFINED_LOGBACK_CONF);
    }

    public static void setUserDefinedLogbackConf(Map conf, String fileName) {
        conf.put(USER_DEFINED_LOGBACK_CONF, fileName);
    }

    protected static String TASK_ERROR_INFO_REPORT_INTERVAL = "topology.task.error.report.interval";

    public static Integer getTaskErrorReportInterval(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_ERROR_INFO_REPORT_INTERVAL), 60);
    }

    public static void setTaskErrorReportInterval(Map conf, Integer interval) {
        conf.put(TASK_ERROR_INFO_REPORT_INTERVAL, interval);
    }

    protected static String DEFAULT_CACHE_TIMEOUT = "default.cache.timeout";

    public static int getDefaultCacheTimeout(Map conf) {
        return JStormUtils.parseInt(conf.get(DEFAULT_CACHE_TIMEOUT), 60);
    }

    public static void setDefaultCacheTimeout(Map conf, int timeout) {
        conf.put(DEFAULT_CACHE_TIMEOUT, timeout);
    }

    protected static String WORKER_MERTRIC_REPORT_CHECK_FREQUENCY = "worker.metric.report.frequency.secs";

    public static int getWorkerMetricReportCheckFrequency(Map conf) {
        return JStormUtils.parseInt(conf.get(WORKER_MERTRIC_REPORT_CHECK_FREQUENCY), 60);
    }

    public static void setWorkerMetricReportFrequency(Map conf, int frequence) {
        conf.put(WORKER_MERTRIC_REPORT_CHECK_FREQUENCY, frequence);
    }

    /**
     * Store local worker port/workerId/supervisorId to configuration
     */
    protected static String LOCAL_WORKER_PORT = "local.worker.port";
    protected static String LOCLA_WORKER_ID = "local.worker.id";
    protected static String LOCAL_SUPERVISOR_ID = "local.supervisor.id";

    public static int getLocalWorkerPort(Map conf) {
        return JStormUtils.parseInt(conf.get(LOCAL_WORKER_PORT));
    }

    public static void setLocalWorkerPort(Map conf, int port) {
        conf.put(LOCAL_WORKER_PORT, port);
    }

    public static String getLocalWorkerId(Map conf) {
        return (String) conf.get(LOCLA_WORKER_ID);
    }

    public static void setLocalWorkerId(Map conf, String workerId) {
        conf.put(LOCLA_WORKER_ID, workerId);
    }

    public static String getLocalSupervisorId(Map conf) {
        return (String) conf.get(LOCAL_SUPERVISOR_ID);
    }

    public static void setLocalSupervisorId(Map conf, String supervisorId) {
        conf.put(LOCAL_SUPERVISOR_ID, supervisorId);
    }

    protected static String WORKER_CPU_CORE_UPPER_LIMIT = "worker.cpu.core.upper.limit";

    public static Integer getWorkerCpuCoreUpperLimit(Map conf) {
        return JStormUtils.parseInt(conf.get(WORKER_CPU_CORE_UPPER_LIMIT), 1);
    }

    public static void setWorkerCpuCoreUpperLimit(Map conf, Integer cpuUpperLimit) {
        conf.put(WORKER_CPU_CORE_UPPER_LIMIT, cpuUpperLimit);
    }


    protected static String CLUSTER_NAME = "cluster.name";

    public static String getClusterName(Map conf) {
        return (String) conf.get(CLUSTER_NAME);
    }

    public static void setClusterName(Map conf, String clusterName) {
        conf.put(CLUSTER_NAME, clusterName);
    }


    protected static final String NIMBUS_CACHE_CLASS = "nimbus.cache.class";

    public static String getNimbusCacheClass(Map conf) {
        return (String) conf.get(NIMBUS_CACHE_CLASS);
    }

    /**
     * if this is set, nimbus cache db will be clean when start nimbus
     */
    protected static final String NIMBUS_CACHE_RESET = "nimbus.cache.reset";

    public static boolean getNimbusCacheReset(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NIMBUS_CACHE_RESET), true);
    }

    /**
     * if this is set, nimbus metrics cache db will be clean when start nimbus
     */
    protected static final String NIMBUS_METRIC_CACHE_RESET = "nimbus.metric.cache.reset";

    public static boolean getMetricCacheReset(Map conf) {
        return JStormUtils.parseBoolean(conf.get(NIMBUS_METRIC_CACHE_RESET), false);
    }

    public static final double DEFAULT_METRIC_SAMPLE_RATE = 0.10d;

    public static final String TOPOLOGY_METRIC_SAMPLE_RATE = "topology.metric.sample.rate";

    public static double getMetricSampleRate(Map conf) {
        double sampleRate = JStormUtils.parseDouble(conf.get(TOPOLOGY_METRIC_SAMPLE_RATE), DEFAULT_METRIC_SAMPLE_RATE);
        if (!conf.containsKey(TOPOLOGY_METRIC_SAMPLE_RATE)) {
            conf.put(TOPOLOGY_METRIC_SAMPLE_RATE, sampleRate);
        }
        return sampleRate;
    }

    public static final String CACHE_TIMEOUT_LIST = "cache.timeout.list";

    public static List<Integer> getCacheTimeoutList(Map conf) {
        return (List<Integer>) conf.get(CACHE_TIMEOUT_LIST);
    }

    protected static final String NIMBUS_METRICS_THREAD_NUM = "nimbus.metrics.thread.num";

    public static int getNimbusMetricThreadNum(Map conf) {
        return JStormUtils.parseInt(conf.get(NIMBUS_METRICS_THREAD_NUM), 2);
    }

    public static final String METRIC_UPLOADER_CLASS = "nimbus.metric.uploader.class";

    public static String getMetricUploaderClass(Map<Object, Object> conf) {
        return (String) conf.get(METRIC_UPLOADER_CLASS);
    }

    public static final String METRIC_QUERY_CLIENT_CLASS = "nimbus.metric.query.client.class";

    public static String getMetricQueryClientClass(Map<Object, Object> conf) {
        return (String) conf.get(METRIC_QUERY_CLIENT_CLASS);
    }

    protected static String TASK_MSG_BATCH_SIZE = "task.msg.batch.size";

    public static Integer getTaskMsgBatchSize(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_MSG_BATCH_SIZE), 1);
    }

    public static void setTaskMsgBatchSize(Map conf, Integer batchSize) {
        conf.put(TASK_MSG_BATCH_SIZE, batchSize);
    }

    protected static String TASK_BATCH_TUPLE = "task.batch.tuple";

    public static Boolean isTaskBatchTuple(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TASK_BATCH_TUPLE), false);
    }

    public static void setTaskBatchTuple(Map conf, boolean isBatchTuple) {
        conf.put(TASK_BATCH_TUPLE, isBatchTuple);
    }

    protected static String TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS = "topology.max.worker.num.for.netty.metrics";

    public static void setTopologyMaxWorkerNumForNettyMetrics(Map conf, int num) {
        conf.put(TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS, num);
    }

    public static int getTopologyMaxWorkerNumForNettyMetrics(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS), 200);
    }

    protected static String UI_ONE_TABLE_PAGE_SIZE = "ui.one.table.page.size";

    public static long getUiOneTablePageSize(Map conf) {
        return JStormUtils.parseLong(conf.get(UI_ONE_TABLE_PAGE_SIZE), 200);
    }

    protected static String MAX_PENDING_METRIC_NUM = "topology.max.pending.metric.num";

    public static int getMaxPendingMetricNum(Map conf) {
        return JStormUtils.parseInt(conf.get(MAX_PENDING_METRIC_NUM), 200);
    }

    protected static String TOPOLOGY_MASTER_SINGLE_WORKER = "topology.master.single.worker";

    public static Boolean getTopologyMasterSingleWorker(Map conf) {
        Boolean ret = JStormUtils.parseBoolean(conf.get(TOPOLOGY_MASTER_SINGLE_WORKER));
        return ret;
    }

    public static String TOPOLOGY_BACKPRESSURE_WATER_MARK_HIGH = "topology.backpressure.water.mark.high";

    public static double getBackpressureWaterMarkHigh(Map conf) {
        return JStormUtils.parseDouble(conf.get(TOPOLOGY_BACKPRESSURE_WATER_MARK_HIGH), 0.8);
    }

    public static String TOPOLOGY_BACKPRESSURE_WATER_MARK_LOW = "topology.backpressure.water.mark.low";

    public static double getBackpressureWaterMarkLow(Map conf) {
        return JStormUtils.parseDouble(conf.get(TOPOLOGY_BACKPRESSURE_WATER_MARK_LOW), 0.05);
    }

    protected static String TOPOLOGY_BACKPRESSURE_CHECK_INTERVAL = "topology.backpressure.check.interval";

    public static int getBackpressureCheckIntervl(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_BACKPRESSURE_CHECK_INTERVAL), 1000);
    }

    protected static String TOPOLOGY_BACKPRESSURE_TRIGGER_SAMPLE_NUMBER = "topology.backpressure.trigger.sample.number";

    public static int getBackpressureTriggerSampleNumber(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_BACKPRESSURE_TRIGGER_SAMPLE_NUMBER), 4);
    }

    protected static String TOPOLOGY_BACKPRESSURE_TRIGGER_SAMPLE_RATE = "topology.backpressure.trigger.sample.rate";

    public static double getBackpressureTriggerSampleRate(Map conf) {
        return JStormUtils.parseDouble(conf.get(TOPOLOGY_BACKPRESSURE_TRIGGER_SAMPLE_RATE), 0.75);
    }

    public static String TOPOLOGY_BACKPRESSURE_ENABLE = "topology.backpressure.enable";

    public static boolean isBackpressureEnable(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_BACKPRESSURE_ENABLE), false);
    }

    public static String TOPOLOGY_BACKPRESSURE_COORDINATOR_RATIO = "topology.backpressure.coordinator.trigger.ratio";

    public static double getBackpressureCoordinatorRatio(Map conf) {
        return JStormUtils.parseDouble(conf.get(TOPOLOGY_BACKPRESSURE_COORDINATOR_RATIO), 0.1);
    }

    protected static String SUPERVISOR_CHECK_WORKER_BY_SYSTEM_INFO = "supervisor.check.worker.by.system.info";

    public static boolean isCheckWorkerAliveBySystemInfo(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_CHECK_WORKER_BY_SYSTEM_INFO), true);
    }

    protected static String TOPOLOGY_TASK_HEARTBEAT_SEND_NUMBER = "topology.task.heartbeat.send.number";

    public static int getTopologyTaskHbSendNumber(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_TASK_HEARTBEAT_SEND_NUMBER), 2000);
    }
}