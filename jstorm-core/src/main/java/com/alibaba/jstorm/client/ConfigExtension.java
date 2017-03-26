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
import com.alibaba.jstorm.config.DefaultConfigUpdateHandler;
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
        conf.put(TOPOLOGY_DEBUG_RECV_TUPLE, debug);
    }

    public static Boolean isTopologyDebugRecvTuple(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_DEBUG_RECV_TUPLE), false);
    }

    private static final String TOPOLOGY_DEBUG_SAMPLE_RATE = "topology.debug.sample.rate";

    public static double getTopologyDebugSampleRate(Map conf) {
        double rate = JStormUtils.parseDouble(conf.get(TOPOLOGY_DEBUG_SAMPLE_RATE), 1.0d);
        if (!conf.containsKey(TOPOLOGY_DEBUG_SAMPLE_RATE)) {
            conf.put(TOPOLOGY_DEBUG_SAMPLE_RATE, rate);
        }
        return rate;
    }

    public static void setTopologyDebugSampleRate(Map conf, double rate) {
        conf.put(TOPOLOGY_DEBUG_SAMPLE_RATE, rate);
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
     * whether to enable stream metrics, in order to reduce metrics data
     */
    public static final String TOPOLOGY_ENABLE_STREAM_METRICS = "topology.enable.stream.metrics";

    public static boolean isEnableStreamMetrics(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_STREAM_METRICS), true);
    }

    /**
     * metric names to be enabled on the fly
     */
    private static final String TOPOLOGY_ENABLED_METRIC_NAMES = "topology.enabled.metric.names";

    public static String getEnabledMetricNames(Map conf) {
        return (String) conf.get(TOPOLOGY_ENABLED_METRIC_NAMES);
    }

    public static void setEnabledMetricNames(Map conf, String metrics) {
        conf.put(TOPOLOGY_ENABLED_METRIC_NAMES, metrics);
    }

    /**
     * metric names to be disabled on the fly
     */
    private static final String TOPOLOGY_DISABLED_METRIC_NAMES = "topology.disabled.metric.names";

    public static String getDisabledMetricNames(Map conf) {
        return (String) conf.get(TOPOLOGY_DISABLED_METRIC_NAMES);
    }

    public static void setDisabledMetricNames(Map conf, String metrics) {
        conf.put(TOPOLOGY_DISABLED_METRIC_NAMES, metrics);
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
            return false;
        return (Boolean) result;
    }

    protected static final String WOREKER_REDIRECT_OUTPUT_FILE = "worker.redirect.output.file";

    public static void setWorkerRedirectOutputFile(Map conf, String outputPath) {
        conf.put(WOREKER_REDIRECT_OUTPUT_FILE, outputPath);
    }

    public static String getWorkerRedirectOutputFile(Map conf) {
        return (String) conf.get(WOREKER_REDIRECT_OUTPUT_FILE);
    }


    protected static final String OUTPUT_WOEKER_DUMP = "output.worker.dump";

    public static boolean isOutworkerDump(Map conf) {
        return JStormUtils.parseBoolean(conf.get(OUTPUT_WOEKER_DUMP), false);
    }

    /**
     * Usually, spout finish prepare before bolt, so spout need wait several seconds so that bolt finish preparation
     * <p/>
     * By default, the setting is 30 seconds
     */
    protected static final String SPOUT_DELAY_RUN = "spout.delay.run";

    public static void setSpoutDelayRunSeconds(Map conf, int delay) {
        conf.put(SPOUT_DELAY_RUN, delay);
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
        conf.put(MEM_SLOTS_PER_TASK, slotNum);
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
        conf.put(CPU_SLOTS_PER_TASK, slotNum);
    }

    /**
     * * *  worker machine minimum available memory (reserved)
     */
    protected static final String STORM_MACHINE_RESOURCE_RESERVE_MEM = " storm.machine.resource.reserve.mem";

    public static long getStormMachineReserveMem(Map conf) {
        Long reserve = JStormUtils.parseLong(conf.get(STORM_MACHINE_RESOURCE_RESERVE_MEM), 1 * 1024 * 1024 * 1024L);
        return reserve < 1 * 1024 * 1024 * 1024L ? 1 * 1024 * 1024 * 1024L : reserve;
    }

    public static void setStormMachineReserveMem(Map conf, long percent) {
        conf.put(STORM_MACHINE_RESOURCE_RESERVE_MEM, Long.valueOf(percent));
    }

    /**
     * * *  worker machine upper boundary on cpu usage
     */
    protected static final String STORM_MACHINE_RESOURCE_RESERVE_CPU_PERCENT = "storm.machine.resource.reserve.cpu.percent";

    public static int getStormMachineReserveCpuPercent(Map conf) {
        int percent = JStormUtils.parseInt(conf.get(STORM_MACHINE_RESOURCE_RESERVE_CPU_PERCENT), 10);
        return percent < 10 ? 10 : percent;
    }

    public static void setStormMachineReserveCpuPercent(Map conf, int percent) {
        conf.put(STORM_MACHINE_RESOURCE_RESERVE_CPU_PERCENT, Integer.valueOf(percent));
    }


    /**
     * if the setting has been set, the component's task must run different node This is conflict with USE_SINGLE_NODE
     */
    protected static final String TASK_ON_DIFFERENT_NODE = "task.on.differ.node";

    public static void setTaskOnDifferentNode(Map conf, boolean isIsolate) {
        conf.put(TASK_ON_DIFFERENT_NODE, isIsolate);
    }

    public static boolean isTaskOnDifferentNode(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TASK_ON_DIFFERENT_NODE), false);
    }

    protected static final String SUPERVISOR_ENABLE_CGROUP = "supervisor.enable.cgroup";

    public static boolean isEnableCgroup(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_ENABLE_CGROUP), false);
    }

    /**
     * supervisor health check
     */
    protected static final String SUPERVISOR_ENABLE_CHECK = "supervisor.enable.check";

    public static boolean isEnableCheckSupervisor(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_ENABLE_CHECK), false);
    }


    protected static String SUPERVISOR_FREQUENCY_CHECK = "supervisor.frequency.check.secs";

    public static int getSupervisorFrequencyCheck(Map conf) {
        return JStormUtils.parseInt(conf.get(SUPERVISOR_FREQUENCY_CHECK), 60);
    }

    protected static final String STORM_HEALTH_CHECK_DIR = "storm.health.check.dir";

    public static String getStormHealthCheckDir(Map conf) {
        return (String) (conf.get(STORM_HEALTH_CHECK_DIR));
    }

    protected static final String STORM_MACHINE_RESOURCE_PANIC_CHECK_DIR = "storm.machine.resource.panic.check.dir";

    public static String getStormMachineResourcePanicCheckDir(Map conf) {
        return (String) (conf.get(STORM_MACHINE_RESOURCE_PANIC_CHECK_DIR));
    }

    protected static final String STORM_MACHINE_RESOURCE_ERROR_CHECK_DIR = "storm.machine.resource.error.check.dir";

    public static String getStormMachineResourceErrorCheckDir(Map conf) {
        return (String) (conf.get(STORM_MACHINE_RESOURCE_ERROR_CHECK_DIR));
    }

    protected static final String STORM_MACHINE_RESOURCE_WARNING_CHECK_DIR = "storm.machine.resource.warning.check.dir";

    public static String getStormMachineResourceWarningCheckDir(Map conf) {
        return (String) (conf.get(STORM_MACHINE_RESOURCE_WARNING_CHECK_DIR));
    }


    protected static final String STORM_HEALTH_CHECK_TIMEOUT_MS = "storm.health.check.timeout.ms";

    public static long getStormHealthTimeoutMs(Map conf) {
        return JStormUtils.parseLong(conf.get(STORM_HEALTH_CHECK_TIMEOUT_MS), 5000);
    }

    protected static final String STORM_HEALTH_CHECK_MAX_DISK_UTILIZATION_PERCENTAGE =
            "storm.health.check.dir.max.disk.utilization.percentage";

    public static float getStormHealthCheckMaxDiskUtilizationPercentage(Map conf) {
        return (float) (conf.get(STORM_HEALTH_CHECK_MAX_DISK_UTILIZATION_PERCENTAGE));
    }

    protected static final String STORM_HEALTH_CHECK_MIN_DISK_FREE_SPACE_MB =
            "storm.health.check.min.disk.free.space.mb";

    public static long getStormHealthCheckMinDiskFreeSpaceMb(Map conf) {
        return (long) (conf.get(STORM_HEALTH_CHECK_MIN_DISK_FREE_SPACE_MB));
    }

    /**
     * If component or topology configuration set "use.old.assignment", will try use old assignment firstly
     */
    protected static final String USE_OLD_ASSIGNMENT = "use.old.assignment";

    public static void setUseOldAssignment(Map conf, boolean useOld) {
        conf.put(USE_OLD_ASSIGNMENT, useOld);
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
        return JStormUtils.parseBoolean(conf.get(NIMBUS_USE_IP), true);
    }

    protected static final String TOPOLOGY_ENABLE_CLASSLOADER = "topology.enable.classloader";

    public static boolean isEnableTopologyClassLoader(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_CLASSLOADER), false);
    }

    public static void setEnableTopologyClassLoader(Map conf, boolean enable) {
        conf.put(TOPOLOGY_ENABLE_CLASSLOADER, enable);
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
        return System.getenv(CONTAINER_SUPERVISOR_HEARTBEAT);
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

    public static String CGROUP_ROOT_DIR = "supervisor.cgroup.rootdir";

    public static String getCgroupRootDir(Map conf) {
        return (String) conf.get(CGROUP_ROOT_DIR);
    }

    public static String CGROUP_BASE_DIR = "supervisor.cgroup.basedir";

    public static String getCgroupBaseDir(Map conf) {
        return (String) conf.get(CGROUP_BASE_DIR);
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
        if (JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 0) == 0) {
            messageTimeout = messageTimeout * 10;
        }
        return JStormUtils.parseLong(conf.get(NETTY_PENDING_BUFFER_TIMEOUT), messageTimeout * 1000);
    }

    protected static final String MEMSIZE_PER_WORKER = "worker.memory.size";

    public static void setMemSizePerWorker(Map conf, long memSize) {
        conf.put(MEMSIZE_PER_WORKER, memSize);
    }

    protected static final String MEMSIZE_PER_TOPOLOGY_MASTER_WORKER = "topology.master.worker.memory.size";

    public static void setMemSizePerTopologyMasterWorker(Map conf, long memSize) {
        conf.put(MEMSIZE_PER_TOPOLOGY_MASTER_WORKER, memSize);
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

    public static Long getMemSizePerTopologyMasterWorker(Map conf) {
        return JStormUtils.parseLong(conf.get(MEMSIZE_PER_TOPOLOGY_MASTER_WORKER));
    }

    protected static final String MIN_MEMSIZE_PER_WORKER = "worker.memory.min.size";

    public static void setMemMinSizePerWorker(Map conf, long memSize) {
        conf.put(MIN_MEMSIZE_PER_WORKER, memSize);
    }

    private static boolean isMemMinSizePerWorkerValid(Long size, long maxMemSize) {
        if (size == null) {
            return false;
        } else if (size <= 128 * 1024 * 1024) {
            return false;
        } else if (size > maxMemSize) {
            return false;
        }

        return true;
    }

    public static long getMemMinSizePerWorker(Map conf) {
        long maxMemSize = getMemSizePerWorker(conf);

        Long size = JStormUtils.parseLong(conf.get(MIN_MEMSIZE_PER_WORKER));
        if (isMemMinSizePerWorkerValid(size, maxMemSize)) {
            return size;
        } else {
            return maxMemSize;
        }
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
        return JStormUtils.parseInt(conf.get(LOG_PAGE_SIZE), 64 * 1024);
    }

    public static void setLogPageSize(Map conf, int pageSize) {
        conf.put(LOG_PAGE_SIZE, pageSize);
    }

    public static String MAX_MATCH_PER_LOG_SEARCH = "max.match.per.log.search";

    public static int getMaxMatchPerLogSearch(Map conf) {
        return JStormUtils.parseInt(conf.get(MAX_MATCH_PER_LOG_SEARCH), 10);
    }

    public static void setMaxMatchPerLogSearch(Map conf, int maxMatch) {
        conf.put(MAX_MATCH_PER_LOG_SEARCH, maxMatch);
    }

    public static String MAX_BLOCKS_PER_LOG_SEARCH = "max.blocks.per.log.search";

    public static int getMaxBlocksPerLogSearch(Map conf) {
        return JStormUtils.parseInt(conf.get(MAX_BLOCKS_PER_LOG_SEARCH), 1024);
    }

    // logger name -> log level map <String, String>
    public static String CHANGE_LOG_LEVEL_CONFIG = "change.log.level.config";

    public static Map<String, String> getChangeLogLevelConfig(Map conf) {
        return (Map<String, String>) conf.get(CHANGE_LOG_LEVEL_CONFIG);
    }

    // this timestamp is used to check whether we need to change log level
    public static String CHANGE_LOG_LEVEL_TIMESTAMP = "change.log.level.timestamp";

    public static Long getChangeLogLevelTimeStamp(Map conf) {
        return (Long) conf.get(CHANGE_LOG_LEVEL_TIMESTAMP);
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

    protected static String SUPERVISOR_ENABLE_AUTO_ADJUST_SLOTS = "supervisor.enable.auto.adjust.slots";

    public static boolean isSupervisorEnableAutoAdjustSlots(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_ENABLE_AUTO_ADJUST_SLOTS), false);
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

    public static final double DEFAULT_METRIC_SAMPLE_RATE = 0.05d;

    public static final String TOPOLOGY_METRIC_SAMPLE_RATE = "topology.metric.sample.rate";

    public static double getMetricSampleRate(Map conf) {
        double sampleRate = JStormUtils.parseDouble(conf.get(TOPOLOGY_METRIC_SAMPLE_RATE), DEFAULT_METRIC_SAMPLE_RATE);
        if (!conf.containsKey(TOPOLOGY_METRIC_SAMPLE_RATE)) {
            conf.put(TOPOLOGY_METRIC_SAMPLE_RATE, sampleRate);
        }
        return sampleRate;
    }

    public static final String TOPOLOGY_TIMER_UPDATE_INTERVAL = "topology.timer.update.interval";

    public static long getTimerUpdateInterval(Map conf) {
        return JStormUtils.parseLong(conf.get(TOPOLOGY_TIMER_UPDATE_INTERVAL), 10);
    }

    public static void setTopologyTimerUpdateInterval(Map conf, long interval) {
        conf.put(TOPOLOGY_TIMER_UPDATE_INTERVAL, interval);
    }

    public static final String CACHE_TIMEOUT_LIST = "cache.timeout.list";

    public static List<Integer> getCacheTimeoutList(Map conf) {
        return (List<Integer>) conf.get(CACHE_TIMEOUT_LIST);
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

    public static String TASK_BATCH_TUPLE = "task.batch.tuple";

    public static Boolean isTaskBatchTuple(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TASK_BATCH_TUPLE), false);
    }

    public static void setTaskBatchTuple(Map conf, boolean isBatchTuple) {
        conf.put(TASK_BATCH_TUPLE, isBatchTuple);
    }

    protected static String TASK_BATCH_FLUSH_INTERVAL_MS = "task.msg.batch.flush.interval.millis";

    public static Integer getTaskMsgFlushInervalMs(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_BATCH_FLUSH_INTERVAL_MS), 2);
    }

    public static void setTaskMsgFlushInervalMs(Map conf, Integer flushMs) {
        conf.put(TASK_BATCH_FLUSH_INTERVAL_MS, flushMs);
    }

    protected static String TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS = "topology.max.worker.num.for.netty.metrics";

    public static void setTopologyMaxWorkerNumForNettyMetrics(Map conf, int num) {
        conf.put(TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS, num);
    }

    public static int getTopologyMaxWorkerNumForNettyMetrics(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_MAX_WORKER_NUM_FOR_NETTY_METRICS), 100);
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

    public static String TOPOLOGY_BACKPRESSURE_ENABLE = "topology.backpressure.enable";

    public static boolean isBackpressureEnable(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_BACKPRESSURE_ENABLE), false);
    }

    protected static String SUPERVISOR_CHECK_WORKER_BY_SYSTEM_INFO = "supervisor.check.worker.by.system.info";

    public static boolean isCheckWorkerAliveBySystemInfo(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SUPERVISOR_CHECK_WORKER_BY_SYSTEM_INFO), true);
    }

    protected static String TOPOLOGY_TASK_HEARTBEAT_SEND_NUMBER = "topology.task.heartbeat.send.number";

    public static int getTopologyTaskHbSendNumber(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_TASK_HEARTBEAT_SEND_NUMBER), 2000);
    }

    protected static String PROCESS_LAUNCHER_ENABLE = "process.launcher.enable";

    public static boolean isProcessLauncherEnable(Map conf) {
        return JStormUtils.parseBoolean(conf.get(PROCESS_LAUNCHER_ENABLE), true);
    }

    protected static String PROCESS_LAUNCHER_SLEEP_SECONDS = "process.launcher.sleep.seconds";

    public static int getProcessLauncherSleepSeconds(Map conf) {
        return JStormUtils.parseInt(conf.get(PROCESS_LAUNCHER_SLEEP_SECONDS), 60);
    }

    protected static String PROCESS_LAUNCHER_CHILDOPTS = "process.launcher.childopts";

    public static String getProcessLauncherChildOpts(Map conf) {
        return (String) conf.get(PROCESS_LAUNCHER_CHILDOPTS);
    }

    protected static String MAX_PENDING_BATCH_SIZE = "max.pending.batch.size";

    public static int getMaxPendingBatchSize(Map conf) {
        return JStormUtils.parseInt(conf.get(MAX_PENDING_BATCH_SIZE), 10 * getTaskMsgBatchSize(conf));
    }

    protected static String TASK_DESERIALIZE_THREAD_NUM = "task.deserialize.thread.num";

    public static Integer getTaskDeserializeThreadNum(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_DESERIALIZE_THREAD_NUM), 1);
    }

    protected static String TASK_SERIALIZE_THREAD_NUM = "task.serialize.thread.num";

    public static Integer getTaskSerializeThreadNum(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_SERIALIZE_THREAD_NUM), 1);
    }

    protected static String WORKER_DESERIALIZE_THREAD_RATIO = "worker.deserialize.thread.ratio";

    public static double getWorkerDeserializeThreadRatio(Map conf) {
        return JStormUtils.parseDouble(conf.get(WORKER_DESERIALIZE_THREAD_RATIO), 0);
    }

    protected static String WORKER_SERIALIZE_THREAD_RATIO = "worker.serialize.thread.ratio";

    public static double getWorkerSerializeThreadRatio(Map conf) {
        return JStormUtils.parseDouble(conf.get(WORKER_SERIALIZE_THREAD_RATIO), 0);
    }

    protected static String DISRUPTOR_BUFFER_SIZE = "disruptor.buffer.size";

    public static int getDisruptorBufferSize(Map conf) {
        return JStormUtils.parseInt(conf.get(DISRUPTOR_BUFFER_SIZE), 10);
    }

    protected static String DISRUPTOR_BUFFER_FLUSH_MS = "disruptor.buffer.interval.millis";

    public static long getDisruptorBufferFlushMs(Map conf) {
        return JStormUtils.parseLong(conf.get(DISRUPTOR_BUFFER_FLUSH_MS), 5);
    }

    protected static String WORKER_FLUSH_POOL_MAX_SIZE = "worker.flush.pool.max.size";

    public static Integer getWorkerFlushPoolMaxSize(Map conf) {
        return JStormUtils.parseInt(conf.get(WORKER_FLUSH_POOL_MAX_SIZE));
    }

    protected static String WORKER_FLUSH_POOL_MIN_SIZE = "worker.flush.pool.min.size";

    public static Integer getWorkerFlushPoolMinSize(Map conf) {
        return JStormUtils.parseInt(conf.get(WORKER_FLUSH_POOL_MIN_SIZE));
    }

    protected static String TRANSACTION_BATCH_SNAPSHOT_TIMEOUT = "transaction.batch.snapshot.timeout";

    public static int getTransactionBatchSnapshotTimeout(Map conf) {
        return JStormUtils.parseInt(conf.get(TRANSACTION_BATCH_SNAPSHOT_TIMEOUT), 120);
    }

    protected static String TOPOLOGY_TRANSACTION_STATE_OPERATOR_CLASS = "topology.transaction.state.operator.class";

    public static String getTopologyStateOperatorClass(Map conf) {
        return (String) conf.get(TOPOLOGY_TRANSACTION_STATE_OPERATOR_CLASS);
    }

    // default is 64k
    protected static String TRANSACTION_CACHE_BATCH_FLUSH_SIZE = "transaction.cache.batch.flush.size";

    public static int getTransactionCacheBatchFlushSize(Map conf) {
        return JStormUtils.parseInt(conf.get(TRANSACTION_CACHE_BATCH_FLUSH_SIZE), 64 * 1024);
    }

    protected static String TRANSACTION_CACHE_BLOCK_SIZE = "transaction.cache.block.size";

    public static Long getTransactionCacheBlockSize(Map conf) {
        return JStormUtils.parseLong(conf.get(TRANSACTION_CACHE_BLOCK_SIZE));
    }

    protected static String TRANSACTION_MAX_CACHE_BLOCK_NUM = "transaction.max.cache.block.num";

    public static Integer getTransactionMaxCacheBlockNum(Map conf) {
        return JStormUtils.parseInt(conf.get(TRANSACTION_MAX_CACHE_BLOCK_NUM));
    }

    protected static final String NIMBUS_CONFIG_UPDATE_HANDLER_CLASS = "nimbus.config.update.handler.class";

    public static String getNimbusConfigUpdateHandlerClass(Map conf) {
        String klass = (String) conf.get(NIMBUS_CONFIG_UPDATE_HANDLER_CLASS);
        if (StringUtils.isBlank(klass)) {
            klass = DefaultConfigUpdateHandler.class.getName();
        }

        return klass;
    }

    public static final String TOPOLOGY_HOT_DEPLOGY_ENABLE = "topology.hot.deploy.enable";

    public static boolean getTopologyHotDeplogyEnable(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_HOT_DEPLOGY_ENABLE), false);
    }

    public static String TASK_CLEANUP_TIMEOUT_SEC = "task.cleanup.timeout.sec";

    public static int getTaskCleanupTimeoutSec(Map conf) {
        return JStormUtils.parseInt(conf.get(TASK_CLEANUP_TIMEOUT_SEC), 10);
    }

    public static void setTaskCleanupTimeoutSec(Map conf, int timeout) {
        conf.put(TASK_CLEANUP_TIMEOUT_SEC, timeout);
    }

    protected static final String SHUFFLE_ENABLE_INTER_PATH = "shuffle.enable.inter.path";

    public static boolean getShuffleEnableInterPath(Map conf) {
        return JStormUtils.parseBoolean(conf.get(SHUFFLE_ENABLE_INTER_PATH), true);
    }

    public static void setShuffleEnableInterPath(Map conf, boolean enable) {
        conf.put(SHUFFLE_ENABLE_INTER_PATH, enable);
    }

    protected static final String SHUFFLE_INTER_LOAD_MARK = "shuffle.inter.load.mark";

    public static double getShuffleInterLoadMark(Map conf) {
        return JStormUtils.parseDouble(conf.get(SHUFFLE_INTER_LOAD_MARK), 0.2);
    }

    public static void setShuffleInterLoadMark(Map conf, double value) {
        conf.put(SHUFFLE_INTER_LOAD_MARK, value);
    }

    protected static final String TOPOLOGY_MASTER_THREAD_POOL_SIZE = "topology.master.thread.pool.size";

    public static int getTopologyMasterThreadPoolSize(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_MASTER_THREAD_POOL_SIZE), 16);
    }

    public static void setTopologyMasterThreadPoolSize(Map conf, int value) {
        conf.put(TOPOLOGY_MASTER_THREAD_POOL_SIZE, value);
    }

    private static final Object CLUSTER_MAX_CONCURRENT_UPLOAD_METRIC_NUM = "cluster.max.concurrent.upload.metric.num";

    public static int getMaxConcurrentUploadingNum(Map conf) {
        return JStormUtils.parseInt(conf.get(CLUSTER_MAX_CONCURRENT_UPLOAD_METRIC_NUM), 20);
    }

    private static final String JSTORM_ON_YARN = "jstorm.on.yarn";

    public static boolean isJStormOnYarn(Map conf) {
        return JStormUtils.parseBoolean(conf.get(JSTORM_ON_YARN), false);
    }

    public static final String NETTY_CLIENT_FLOW_CTRL_WAIT_TIME_MS = "netty.client.flow.ctrl.wait.time.ms";

    public static int getNettyFlowCtrlWaitTime(Map conf) {
    	return JStormUtils.parseInt(conf.get(NETTY_CLIENT_FLOW_CTRL_WAIT_TIME_MS), 5);
    }

    public static final String NETTY_CLIENT_FLOW_CTRL_CACHE_SIZE = "netty.client.flow.ctrl.cache.size";

    public static Integer getNettyFlowCtrlCacheSize(Map conf) {
    	return JStormUtils.parseInt(conf.get(NETTY_CLIENT_FLOW_CTRL_CACHE_SIZE));
    }

    public static final String DISRUPTOR_QUEUE_BATCH_MODE = "disruptor.queue.batch.mode";

    public static Boolean isDisruptorQueueBatchMode(Map conf) {
    	return JStormUtils.parseBoolean(conf.get(DISRUPTOR_QUEUE_BATCH_MODE), isTaskBatchTuple(conf));
    }

    public static String TOPOLOGY_ACCURATE_METRIC = "topology.accurate.metric";
    public static Boolean getTopologyAccurateMetric(Map conf) {
        return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ACCURATE_METRIC), false);
    }

    public static String TOPOLOGY_HISTOGRAM_SIZE = "topology.histogram.size";
    public static Integer getTopologyHistogramSize(Map conf) {
        return JStormUtils.parseInt(conf.get(TOPOLOGY_HISTOGRAM_SIZE), 256);
    }
}