/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.supervisor;

import com.alibaba.jstorm.cluster.StormClusterState;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.alibaba.jstorm.daemon.worker.*;
import com.alibaba.jstorm.oversold.GcStrategy;
import com.alibaba.jstorm.oversold.GcStrategyManager;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.utils.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;

/**
 * SyncProcessesEvent:
 * 1. kill bad workers
 * 2. start new workers
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */
@SuppressWarnings({"unused", "unchecked"})
class SyncProcessEvent extends ShutdownWork {
    private static Logger LOG = LoggerFactory.getLogger(SyncProcessEvent.class);

    private LocalState localState;
    private Map conf;
    private ConcurrentHashMap<String, String> workerThreadPids;
    private String supervisorId;
    private IContext sharedContext;
    private CgroupManager cgroupManager;
    private SandBoxMaker sandBoxMaker;

    /**
     * Due to the worker startTime is put in Supervisor memory, When supervisor restart, the starting worker is likely to be killed
     */
    private Map<String, Pair<Integer, Integer>> workerIdToStartTimeAndPort;
    /**
     * workerIdToStartTimeAndPort ensure workId is unique, but don't ensure workId for workerPort
     */
    private Map<Integer, String> portToWorkerId = new HashMap<>();

    private AtomicReference<Set> needDownloadTopologies;

    private boolean isJstormOnYarn;

    // workers under killing, Map<workerId, time of kill start>;
    private Map<String, Integer> killingWorkers;
    private Map<String, Set<Pair<String, Integer>>> upgradingTopologyPorts;

    // private Supervisor supervisor;
    private int lastTime;

    private WorkerReportError workerReportError;
    private StormClusterState stormClusterState;

    public SyncProcessEvent(String supervisorId, Map conf, LocalState localState,
                            ConcurrentHashMap<String, String> workerThreadPids,
                            IContext sharedContext, WorkerReportError workerReportError,
                            StormClusterState stormClusterState) {
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.localState = localState;
        this.workerThreadPids = workerThreadPids;

        // right now, sharedContext is null
        this.sharedContext = sharedContext;
        this.sandBoxMaker = new SandBoxMaker(conf);
        this.workerIdToStartTimeAndPort = new HashMap<>();
        this.needDownloadTopologies = new AtomicReference<>();
        this.isJstormOnYarn = JStormUtils.parseBoolean(System.getProperty("jstorm-on-yarn"), false) ||
                ConfigExtension.isJStormOnYarn(conf);
        if (ConfigExtension.isEnableCgroup(conf)) {
            cgroupManager = new CgroupManager(conf);
        }
        this.killingWorkers = new HashMap<>();
        this.upgradingTopologyPorts = new HashMap<>();
        this.workerReportError = workerReportError;
        this.stormClusterState = stormClusterState;
    }

    /**
     * Note: Changed the old logic. In the old logic, it will store LS_LOCAL_ASSIGNMENTS Map<String, Integer>
     * into LocalState but I don't think LS_LOCAL_ASSIGNMENTS is useful
     */
    @Override
    public void run() {
    }

    public void run(Map<Integer, LocalAssignment> localAssignments,
                    Set<String> downloadFailedTopologyIds,
                    Map<String, Set<Pair<String, Integer>>> upgradeTopologyPorts) {
        LOG.debug("Syncing processes, interval (sec): " + TimeUtils.time_delta(lastTime));
        lastTime = TimeUtils.current_time_secs();
        try {

            /**
             * Step 1: get assigned tasks from localstat Map<port(type Integer), LocalAssignment>
             */
            if (localAssignments == null) {
                localAssignments = new HashMap<>();
            }
            LOG.debug("Assigned tasks: " + localAssignments);

            /**
             * Step 2: get local WorkerStats from local_dir/worker/ids/heartbeat Map<workerid [WorkerHeartbeat, state]>
             */
            Map<String, StateHeartbeat> localWorkerStats;
            try {
                localWorkerStats = getLocalWorkerStats(conf, localState, localAssignments);
            } catch (Exception e) {
                LOG.error("Failed to get local worker stats");
                throw e;
            }
            LOG.debug("Allocated: " + localWorkerStats);

            /**
             * Step 3: kill Invalid Workers and remove killed worker from localWorkerStats
             */
            Map<String, Integer> taskCleaupTimeoutMap;
            Set<Integer> keepPorts = null;
            try {
                taskCleaupTimeoutMap = (Map<String, Integer>) localState.get(Common.LS_TASK_CLEANUP_TIMEOUT);
                keepPorts = killUselessWorkers(localWorkerStats, localAssignments, taskCleaupTimeoutMap);
                localState.put(Common.LS_TASK_CLEANUP_TIMEOUT, taskCleaupTimeoutMap);
            } catch (IOException e) {
                LOG.error("Failed to kill workers", e);
            }

            // check new workers
            checkNewWorkers(conf);

            // check which topology need update
            checkNeedUpdateTopologies(localWorkerStats, localAssignments);

            // start new workers
            startNewWorkers(keepPorts, localAssignments, downloadFailedTopologyIds);

            // restart upgraded workers
            restartUpgradingWorkers(localAssignments, localWorkerStats, upgradeTopologyPorts);

        } catch (Exception e) {
            LOG.error("Failed to init SyncProcessEvent", e);
            // throw e
        }
    }

    private void restartUpgradingWorkers(Map<Integer, LocalAssignment> assignments,
                                         Map<String, StateHeartbeat> localWorkerStats,
                                         Map<String, Set<Pair<String, Integer>>> upgradeTopologyPorts) throws Exception {
        if (upgradeTopologyPorts.size() == 0) {
            return;
        }

        Map<Integer, String> portToWorkerId = new HashMap<>();
        for (Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {
            String workerId = entry.getKey();
            StateHeartbeat heartbeat = entry.getValue();
            if (heartbeat != null) {
                portToWorkerId.put(heartbeat.getHeartbeat().getPort(), workerId);
            }
        }

        Map<String, String> workerIdToTopology = new HashMap<>();
        Map<String, String> workerIdToHostPort = new HashMap<>();
        for (Entry<String, Set<Pair<String, Integer>>> entry : upgradeTopologyPorts.entrySet()) {
            String topologyId = entry.getKey();
            Set<Pair<String, Integer>> hostPorts = entry.getValue();
            for (Pair<String, Integer> hostPort : hostPorts) {
                String workerId = portToWorkerId.get(hostPort.getSecond());
                if (workerId == null) {
                    LOG.error("Failed to get worker id for port:{}", hostPort.getSecond());
                } else {
                    Set<Pair<String, Integer>> upgradingHostPorts = upgradingTopologyPorts.get(topologyId);
                    if (upgradingHostPorts == null) {
                        upgradingHostPorts = new HashSet<>();
                        upgradingTopologyPorts.put(topologyId, upgradingHostPorts);
                    }
                    if (!upgradingHostPorts.contains(hostPort)) {
                        killingWorkers.put(workerId, TimeUtils.current_time_secs());
                        workerIdToTopology.put(workerId, topologyId);
                        workerIdToHostPort.put(workerId,
                                topologyId + "[" + hostPort.getFirst() + ":" + hostPort.getSecond() + "]");
                        upgradingHostPorts.add(hostPort);
                    }
                }
            }
        }

        LOG.info("\n***********************************************\n" +
                "Upgrading following workers:{}\n" +
                "***********************************************", workerIdToHostPort);
        shutWorker(conf, supervisorId, workerIdToTopology, null, null, false, null, null);

        Map<Integer, LocalAssignment> portToAssignment = new HashMap<>();
        for (Set<Pair<String, Integer>> hostPorts : upgradeTopologyPorts.values()) {
            for (Pair<String, Integer> hostPort : hostPorts) {
                Integer port = hostPort.getSecond();
                LocalAssignment assignment = assignments.get(port);
                if (assignment == null) {
                    LOG.error("Failed to get local assignment for port:{}", port);
                } else {
                    portToAssignment.put(port, assignment);
                }
            }
        }

        LOG.info("Starting workers...");
        startWorkers(portToAssignment);

        LOG.info("Updating upgrading workers on zk...");
        for (Entry<String, Set<Pair<String, Integer>>> entry : upgradeTopologyPorts.entrySet()) {
            String topologyId = entry.getKey();
            for (Pair<String, Integer> hostPort : entry.getValue()) {
                String hp = hostPort.getFirst() + ":" + hostPort.getSecond();
                stormClusterState.remove_upgrading_worker(topologyId, hp);
                stormClusterState.add_upgraded_worker(topologyId, hp);
            }
        }
        upgradingTopologyPorts.clear();
    }

    /**
     * check all workers is failed or not
     */
    @SuppressWarnings("unchecked")
    private void checkNeedUpdateTopologies(Map<String, StateHeartbeat> localWorkerStats,
                                           Map<Integer, LocalAssignment> localAssignments) throws Exception {
        Set<String> topologies = new HashSet<>();

        for (Map.Entry<Integer, LocalAssignment> entry : localAssignments.entrySet()) {
            topologies.add(entry.getValue().getTopologyId());
        }

        for (StateHeartbeat stateHb : localWorkerStats.values()) {
            State state = stateHb.getState();
            if (!state.equals(State.notStarted)) {
                String topologyId = stateHb.getHeartbeat().getTopologyId();
                topologies.remove(topologyId);
            }
        }
        long currTime = System.currentTimeMillis();
        Set<String> needRemoveTopologies = new HashSet<>();
        for (String topologyId : topologies) {
            try {
                long lastModifyTime = StormConfig.get_supervisor_topology_Bianrymodify_time(conf, topologyId);
                if ((currTime - lastModifyTime) / 1000 < (JStormUtils.MIN_1 * 2)) {
                    LOG.debug("less than 2 minute, removing " + topologyId);
                    needRemoveTopologies.add(topologyId);
                }
            } catch (Exception e) {
                LOG.error("Failed to get last modified time for topology" + topologyId, e);
                needRemoveTopologies.add(topologyId);
            }
        }
        topologies.removeAll(needRemoveTopologies);

        if (topologies.size() > 0) {
            LOG.debug("Following topologies are going to re-download jars, " + topologies);
        }
        needDownloadTopologies.set(topologies);
    }

    /**
     * mark all new Workers like 52b11418-7474-446d-bff5-0ecd68f4954f
     */
    public void markAllNewWorkers(Map<Integer, String> workerIds) {
        int startTime = TimeUtils.current_time_secs();

        for (Entry<Integer, String> entry : workerIds.entrySet()) {
            String oldWorkerIds = portToWorkerId.get(entry.getKey());
            if (oldWorkerIds != null) {
                workerIdToStartTimeAndPort.remove(oldWorkerIds);
                // update portToWorkerId
                LOG.info("A port is still occupied by an old worker, remove useless "
                        + oldWorkerIds + " from workerIdToStartTimeAndPort");
            }
            portToWorkerId.put(entry.getKey(), entry.getValue());
            workerIdToStartTimeAndPort.put(entry.getValue(), new Pair<>(startTime, entry.getKey()));
        }
    }

    /**
     * check whether timestamp of new workers is not > SUPERVISOR_WORKER_START_TIMEOUT_SECS, otherwise mark as failed
     */
    public void checkNewWorkers(Map conf) throws IOException, InterruptedException {
        Set<String> workers = new HashSet<>();
        for (Entry<String, Pair<Integer, Integer>> entry : workerIdToStartTimeAndPort.entrySet()) {
            String workerId = entry.getKey();
            int startTime = entry.getValue().getFirst();
            LocalState ls = StormConfig.worker_state(conf, workerId);
            WorkerHeartbeat whb = (WorkerHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);
            if (whb == null) {
                if ((TimeUtils.current_time_secs() - startTime) <
                        JStormUtils.parseInt(conf.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS))) {
                    LOG.info(workerId + " still hasn't started");
                } else {
                    LOG.error("Failed to start Worker " + workerId);
                    workers.add(workerId);
                }
            } else {
                LOG.info("Successfully started worker " + workerId);
                workers.add(workerId);
            }
        }
        for (String workerId : workers) {
            Integer port = this.workerIdToStartTimeAndPort.get(workerId).getSecond();
            this.workerIdToStartTimeAndPort.remove(workerId);
            this.portToWorkerId.remove(port);
        }
    }

    public Map<Integer, String> getPortToWorkerId() {
        return portToWorkerId;
    }

    /**
     * get localStat of approved workerId's map
     *
     * @return Map[workerId, [worker heartbeat, state]]
     */
    public Map<String, StateHeartbeat> getLocalWorkerStats(
            Map conf, LocalState localState, Map<Integer, LocalAssignment> assignedTasks) throws Exception {
        Map<String, StateHeartbeat> workerIdHbState = new HashMap<>();
        int now = TimeUtils.current_time_secs();

        /**
         * Get Map<workerId, WorkerHeartbeat> from local_dir/worker/ids/heartbeat
         */
        Map<String, WorkerHeartbeat> idToHeartbeat = readWorkerHeartbeats(conf);
        for (Map.Entry<String, WorkerHeartbeat> entry : idToHeartbeat.entrySet()) {
            String workerId = entry.getKey();
            WorkerHeartbeat whb = entry.getValue();
            State state;

            if (whb == null) {
                state = State.notStarted;
                Pair<Integer, Integer> timeToPort = this.workerIdToStartTimeAndPort.get(workerId);
                if (timeToPort != null) {
                    LocalAssignment localAssignment = assignedTasks.get(timeToPort.getSecond());
                    if (localAssignment == null) {
                        LOG.info("Following worker doesn't exist in the assignment, remove port=" + timeToPort.getSecond());
                        state = State.disallowed;
                        // workerId is disallowed ,so remove it from workerIdToStartTimeAndPort
                        Integer port = this.workerIdToStartTimeAndPort.get(workerId).getSecond();
                        this.workerIdToStartTimeAndPort.remove(workerId);
                        this.portToWorkerId.remove(port);
                    }
                }
            } else if (!matchesAssignment(whb, assignedTasks)) {
                // workerId isn't approved or isn't assigned task
                state = State.disallowed;
            } else if ((now - whb.getTimeSecs()) > JStormUtils.parseInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS))) {
                if (!killingWorkers.containsKey(workerId)) {
                    String outTimeInfo = " it is likely to be out of memory, the worker heartbeat has timed out ";
                    workerReportError.report(whb.getTopologyId(), whb.getPort(),
                            whb.getTaskIds(), outTimeInfo, ErrorConstants.CODE_WORKER_TIMEOUT);
                }

                state = State.timedOut;
            } else {
                if (isWorkerDead(workerId)) {
                    if (!killingWorkers.containsKey(workerId)) {
                        String workerDeadInfo = "Worker is dead ";
                        workerReportError.report(whb.getTopologyId(), whb.getPort(),
                                whb.getTaskIds(), workerDeadInfo, ErrorConstants.CODE_WORKER_DEAD);
                    }
                    state = State.timedOut;
                } else {
                    state = State.valid;
                }
            }

            if (state != State.valid) {
                if (!killingWorkers.containsKey(workerId))
                    LOG.info("Worker:" + workerId + " state:" + state + " WorkerHeartbeat:" + whb +
                            " assignedTasks:" + assignedTasks + " at supervisor time-secs " + now);
            } else {
                LOG.debug("Worker:" + workerId + " state:" + state + " WorkerHeartbeat: " + whb
                        + " at supervisor time-secs " + now);
            }

            workerIdHbState.put(workerId, new StateHeartbeat(state, whb));
        }

        return workerIdHbState;
    }

    /**
     * check whether the worker heartbeat is allowed in the assignedTasks
     *
     * @param whb           WorkerHeartbeat
     * @param assignedTasks assigned tasks
     * @return if true, the assignments(LS-LOCAL-ASSIGNMENTS) match with worker heartbeat, false otherwise
     */
    public boolean matchesAssignment(WorkerHeartbeat whb, Map<Integer, LocalAssignment> assignedTasks) {
        boolean isMatch = true;
        LocalAssignment localAssignment = assignedTasks.get(whb.getPort());
        if (localAssignment == null) {
            LOG.debug("Following worker has been removed, port=" + whb.getPort() + ", assignedTasks=" + assignedTasks);
            isMatch = false;
        } else if (!whb.getTopologyId().equals(localAssignment.getTopologyId())) {
            // topology id not equal
            LOG.info("topology id not equal whb=" + whb.getTopologyId() + ",localAssignment=" + localAssignment.getTopologyId());
            isMatch = false;
        }/*
          * else if (!(whb.getTaskIds().equals(localAssignment.getTaskIds()))) { // task-id isn't equal LOG.info("task-id isn't equal whb=" + whb.getTaskIds() +
          * ",localAssignment=" + localAssignment.getTaskIds()); isMatch = false; }
          */

        return isMatch;
    }

    /**
     * get all workers heartbeats of the supervisor
     *
     * @param conf conf
     * @throws IOException
     */
    public Map<String, WorkerHeartbeat> readWorkerHeartbeats(Map conf) throws Exception {
        Map<String, WorkerHeartbeat> workerHeartbeats = new HashMap<>();

        // get the path: STORM-LOCAL-DIR/workers
        String path = StormConfig.worker_root(conf);

        List<String> workerIds = PathUtils.read_dir_contents(path);

        if (workerIds == null) {
            LOG.info("No worker dir under " + path);
            return workerHeartbeats;
        }

        for (String workerId : workerIds) {
            WorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }

    /**
     * get worker heartbeat by workerId
     *
     * @param conf     conf
     * @param workerId worker id
     * @return WorkerHeartbeat
     */
    public WorkerHeartbeat readWorkerHeartbeat(Map conf, String workerId) throws Exception {
        try {
            LocalState ls = StormConfig.worker_state(conf, workerId);
            return (WorkerHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);
        } catch (Exception e) {
            LOG.error("Failed to get heartbeat for worker:{}", workerId, e);
            return null;
        }
    }

    /**
     * launch a worker in local mode
     */
    public void launchWorker(Map conf, IContext sharedContext, String topologyId,
                             String supervisorId, Integer port, String workerId,
                             ConcurrentHashMap<String, String> workerThreadPidsAtom) throws Exception {
        String pid = UUID.randomUUID().toString();
        WorkerShutdown worker = Worker.mk_worker(conf, sharedContext, topologyId, supervisorId, port, workerId, null);
        ProcessSimulator.registerProcess(pid, worker);
        workerThreadPidsAtom.put(workerId, pid);
    }

    // filter conflict jar
    private Set<String> setFilterJars(Map totalConf) {
        Set<String> filterJars = new HashSet<>();

        boolean enableClassLoader = ConfigExtension.isEnableTopologyClassLoader(totalConf);
        if (!enableClassLoader) {
            // avoid logback vs log4j conflict
            boolean enableLog4j = false;
            String userDefLog4jConf = ConfigExtension.getUserDefinedLog4jConf(totalConf);
            if (!StringUtils.isBlank(userDefLog4jConf)) {
                enableLog4j = true;
            }

            if (enableLog4j) {
                filterJars.add("log4j-over-slf4j");
                filterJars.add("logback-core");
                filterJars.add("logback-classic");

            } else {
                filterJars.add("slf4j-log4j");
                filterJars.add("log4j");
            }
        }

        String excludeJars = (String) totalConf.get("exclude.jars");
        if (!StringUtils.isBlank(excludeJars)) {
            String[] jars = excludeJars.split(",");
            for (String jar : jars) {
                filterJars.add(jar);
            }

        }

        LOG.info("Remove jars " + filterJars);
        return filterJars;
    }

    public static boolean isKeyContain(Collection<String> collection, String jar) {
        if (collection == null) {
            return false;
        }
        File file = new File(jar);
        String fileName = file.getName();
        for (String item : collection) {
            String regex = item + "[-._0-9]*.jar";
            Pattern p = Pattern.compile(regex);

            if (p.matcher(fileName).matches()) {
                return true;
            }
        }
        return false;
    }

    public AtomicReference<Set> getTopologyIdNeedDownload() {
        return needDownloadTopologies;
    }

    private String getClassPath(String stormHome, Map totalConf) {
        // String classpath = JStormUtils.current_classpath() + ":" + stormjar;
        // return classpath;
        String classpath = JStormUtils.current_classpath();
        String[] classPaths = classpath.split(":");
        Set<String> classSet = new HashSet<>();
        for (String classJar : classPaths) {
            if (StringUtils.isBlank(classJar)) {
                continue;
            }
            if (!classJar.contains("/lib/ext/"))
                classSet.add(classJar);
        }

        if (stormHome != null) {
            List<String> stormHomeFiles = PathUtils.read_dir_contents(stormHome);

            for (String file : stormHomeFiles) {
                if (file.endsWith(".jar")) {
                    classSet.add(stormHome + File.separator + file);
                }
            }

            List<String> stormLibFiles = PathUtils.read_dir_contents(stormHome + File.separator + "lib");
            for (String file : stormLibFiles) {
                if (file.endsWith(".jar")) {
                    classSet.add(stormHome + File.separator + "lib" + File.separator + file);
                }
            }
        }
        Set<String> filterJars = setFilterJars(totalConf);
        StringBuilder sb = new StringBuilder();
        for (String jar : classSet) {
            if (isKeyContain(filterJars, jar)) {
                LOG.info("Remove " + jar);
                continue;
            }
            sb.append(jar).append(":");
        }
        return sb.toString();
    }

    public String getWorkerClassPath(String stormJar, Map totalConf, String stormRoot) {
        StringBuilder sb = new StringBuilder();

        if (!StringUtils.isBlank((String) totalConf.get(Config.TOPOLOGY_CLASSPATH))) {
            sb.append(totalConf.get(Config.TOPOLOGY_CLASSPATH)).append(":");
        }
        List<String> otherLibs = (List<String>) totalConf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
        if (otherLibs != null) {
            for (String libName : otherLibs) {
                sb.append(StormConfig.stormlib_path(stormRoot, libName)).append(":");
            }
        }
        sb.append(stormJar);
        return sb.toString();
    }

    private String getExternalClassPath(String stormHome, Map totalConf) {
        StringBuilder sb = new StringBuilder();
        String extCPs = (String) totalConf.get("worker.external");
        if (!StringUtils.isBlank(extCPs)) {
            String[] exts = extCPs.split(",");
            int len = exts.length;
            for (int i = 0; i < len; i++) {
                sb.append(stormHome + "/lib/ext/" + exts[i] + "/*");
                if (i < len - 1) {
                    sb.append(":");
                }
            }
        }
        return sb.toString();
    }

    public String getChildOpts(Map stormConf) {
        String childOpts = " ";

        if (stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) != null) {
            childOpts += (String) stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
        } else if (ConfigExtension.getWorkerGc(stormConf) != null) {
            childOpts += ConfigExtension.getWorkerGc(stormConf);
        }

        return childOpts;
    }

    public String getLogParameter(Map conf, String stormHome, String topologyName, int port) {
        final String LOGBACK_CONF_TAG = "logback.configurationFile";
        final String LOGBACK_CONF_TAG_CMD = " -D" + LOGBACK_CONF_TAG + "=";
        final String DEFAULT_LOG_CONF = "jstorm.logback.xml";

        String logFileName = JStormUtils.genLogName(topologyName, port);
        // String logFileName = topologyId + "-worker-" + port + ".log";


        StringBuilder commandSB = new StringBuilder();
        String logDir = System.getProperty("jstorm.log.dir");
        if (!StringUtils.isBlank(logDir)) {
            commandSB.append(" -Djstorm.log.dir=").append(logDir);
        }
        commandSB.append(" -Dlogfile.name=").append(logFileName);
        commandSB.append(" -Dtopology.name=").append(topologyName);

        // commandSB.append(" -Dlog4j.ignoreTCL=true");

        String userDefLogbackConf = ConfigExtension.getUserDefinedLogbackConf(conf);
        String logConf = System.getProperty(LOGBACK_CONF_TAG);

        if (!StringUtils.isBlank(userDefLogbackConf)) {
            LOG.info("Use user defined logback conf " + userDefLogbackConf);
            commandSB.append(LOGBACK_CONF_TAG_CMD).append(userDefLogbackConf);
        } else if (!StringUtils.isBlank(logConf)) {
            commandSB.append(LOGBACK_CONF_TAG_CMD).append(logConf);
        } else if (!StringUtils.isBlank(stormHome)) {
            commandSB.append(LOGBACK_CONF_TAG_CMD).append(stormHome).append(File.separator)
                    .append("conf").append(File.separator).append(DEFAULT_LOG_CONF);
        } else {
            commandSB.append(LOGBACK_CONF_TAG_CMD + DEFAULT_LOG_CONF);
        }

        final String LOG4J_CONF_TAG = "log4j.configuration";
        String userDefLog4jConf = ConfigExtension.getUserDefinedLog4jConf(conf);
        if (!StringUtils.isBlank(userDefLog4jConf)) {
            LOG.info("Use user defined log4j conf " + userDefLog4jConf);
            commandSB.append(" -D" + LOG4J_CONF_TAG + "=").append(userDefLog4jConf);
        }

        return commandSB.toString();
    }

    private String getGcDumpParam(String topologyId, int port, Map totalConf) {
        Date now = new Date();
        String nowStr = TimeFormat.getSecond(now);

        String topologyName = Common.getTopologyNameById(topologyId);
        String logPath = JStormUtils.getLogDir();
        String gcPath = PathUtils.join(logPath, topologyName);

        try {
            FileUtils.forceMkdir(new File(gcPath));
        } catch (Exception ignored) {
        }

        String gcLogFile = PathUtils.join(logPath, topologyName, topologyName + "-worker-" + port + "-gc.log");
        String dumpFile = PathUtils.join(logPath, topologyName, "java-" + topologyId + "-" + nowStr + ".hprof");

        // backup old gc log file
        PathUtils.mv(gcLogFile, gcLogFile + ".old");

        StringBuilder gc = new StringBuilder(256);
        gc.append(" -Xloggc:").append(gcLogFile)
                .append(" -verbose:gc -XX:+PrintGCDateStamps -XX:+PrintGCDetails")
                .append(" -XX:HeapDumpPath=").append(dumpFile).append(" ");

        return gc.toString().replace("/./", "/");
    }

    /**
     * Get worker's JVM memory setting
     */
    public String getWorkerMemParameter(LocalAssignment assignment,
                                        Map totalConf,
                                        String topologyId,
                                        Integer port) {
        long memSize = assignment.getMem();
        long memMinSize = ConfigExtension.getMemMinSizePerWorker(totalConf);
        long memGsize = memSize / JStormUtils.SIZE_1_G;
        int gcThreadsNum = memGsize > 4 ? (int) (memGsize * 1.5) : 4;
        String childOpts = getChildOpts(totalConf);
        childOpts += getGcDumpParam(topologyId, port, totalConf);

        StringBuilder commandSB = new StringBuilder();
        memMinSize = memMinSize < memSize ? memMinSize : memSize;
        commandSB.append(" -Xms").append(memMinSize).append(" -Xmx").append(memSize).append(" ");
        if (memMinSize <= (memSize / 2)) {
            commandSB.append(" -Xmn").append(memMinSize / 2).append(" ");
        } else {
            commandSB.append(" -Xmn").append(memSize / 2).append(" ");
        }

        // PermSize is deprecated in jdk1.8
        if (JDKInfo.isJdk7() || JDKInfo.lessThanJdk7()) {
            if (memGsize >= 2) {
                commandSB.append(" -XX:PermSize=").append(memSize / 32);
            } else {
                commandSB.append(" -XX:PermSize=").append(memSize / 16);
            }
            commandSB.append(" -XX:MaxPermSize=").append(memSize / 16);
        } else if (JDKInfo.isJdk8()) {
            if (memGsize >= 2) {
                commandSB.append(" -XX:MetaspaceSize=").append(memSize / 32);
            } else {
                commandSB.append(" -XX:MetaspaceSize=").append(memSize / 16);
            }
            commandSB.append(" -XX:MaxMetaspaceSize=").append(memSize / 16);
        }

        commandSB.append(" -XX:ParallelGCThreads=").append(gcThreadsNum);
        commandSB.append(" ").append(childOpts);
        if (!StringUtils.isBlank(assignment.getJvm())) {
            commandSB.append(" ").append(assignment.getJvm());
        }

        return commandSB.toString();
    }

    /**
     * Get worker's JVM gc setting
     */
    public String getWorkerGcParameter(LocalAssignment assignment,
                                       Map totalConf,
                                       String topologyId,
                                       Integer port) {

        StringBuilder commandSB = new StringBuilder();
        GcStrategy gcStrategy = GcStrategyManager.getGcStrategy(totalConf);
        return gcStrategy.toString();
    }

    public String getSandBoxParameter(String classpath, String workerClassPath, String workerId)
            throws IOException {
        Map<String, String> policyReplaceMap = new HashMap<>();
        String realClassPath = classpath + ":" + workerClassPath;
        policyReplaceMap.put(SandBoxMaker.CLASS_PATH_KEY, realClassPath);
        return sandBoxMaker.sandboxPolicy(workerId, policyReplaceMap);
    }

    public String getWorkerParameter(LocalAssignment assignment,
                                     Map totalConf,
                                     String stormHome,
                                     String topologyId,
                                     String supervisorId,
                                     String workerId,
                                     Integer port) throws IOException {
        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId
        String stormRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);

        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId/stormjar.jar
        String stormJar = StormConfig.stormjar_path(stormRoot);

        StringBuilder commandSB = new StringBuilder();
        try {
            if (this.cgroupManager != null) {
                commandSB.append(cgroupManager.startNewWorker(totalConf, assignment.getCpu(), workerId));
            }
        } catch (Exception e) {
            LOG.error("Failed to prepare cgroup for workerId: " + workerId, e);
            commandSB = new StringBuilder();
        }
        commandSB.append("java -server ");
        commandSB.append(getWorkerMemParameter(assignment, totalConf, topologyId, port));
        commandSB.append(getWorkerGcParameter(assignment, totalConf, topologyId, port));

        commandSB.append(" -Djava.library.path=").append((String) totalConf.get(Config.JAVA_LIBRARY_PATH));
        commandSB.append(" -Djstorm.home=").append(stormHome);
        commandSB.append(getLogParameter(totalConf, stormHome, assignment.getTopologyName(), port));

        // get child process parameter
        String classpath = getClassPath(stormHome, totalConf);
        String workerClassPath = getWorkerClassPath(stormJar, totalConf, stormRoot);
        String externalClassPath = getExternalClassPath(stormHome, totalConf);

        commandSB.append(getSandBoxParameter(classpath, workerClassPath, workerId));

        commandSB.append(" -cp ");
        // commandSB.append(workerClassPath + ":");
        commandSB.append(classpath);
        if (!ConfigExtension.isEnableTopologyClassLoader(totalConf))
            commandSB.append(":").append(workerClassPath);
        if (!StringUtils.isBlank(externalClassPath)) {
            commandSB.append(":").append(externalClassPath);
        }

        //todo: use worker fqn
        commandSB.append(" com.alibaba.jstorm.daemon.worker.Worker ");
        commandSB.append(topologyId);
        commandSB.append(" ").append(supervisorId);
        commandSB.append(" ").append(port);
        commandSB.append(" ").append(workerId);
        commandSB.append(" ").append(workerClassPath);

        return commandSB.toString();

    }

    public String getLauncherParameter(
            LocalAssignment assignment, Map totalConf, String stormHome, String topologyId, int port) throws IOException {
        boolean isEnable = ConfigExtension.isProcessLauncherEnable(totalConf);
        if (!isEnable) {
            return "";
        }

        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId
        String stormRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);
        // STORM-LOCAL-DIR/supervisor/stormdist/topologyId/stormjar.jar
        String stormJar = StormConfig.stormjar_path(stormRoot);

        StringBuilder sb = new StringBuilder();
        sb.append(" java ");
        sb.append(ConfigExtension.getProcessLauncherChildOpts(totalConf));
        sb.append(getLogParameter(totalConf, stormHome, assignment.getTopologyName(), port));
        sb.append(" -cp ");
        sb.append(getClassPath(stormHome, totalConf));
        if (ConfigExtension.isEnableTopologyClassLoader(totalConf)) {
            // don't append storm jar when classloader is enabled
        } else {
            // user defined log configuration is likely to be bundled in the storm jar
            sb.append(":").append(stormJar);
        }
        sb.append(" ").append(ProcessLauncher.class.getName()).append(" ");

        String launcherCmd = sb.toString();
        if (ConfigExtension.getWorkerRedirectOutput(totalConf)) {
            String outFile = getWorkerRedirectOutput(totalConf, assignment, port);
            outFile = "-Dlogfile.name=" + outFile + " ";
            launcherCmd = launcherCmd.replaceAll("-Dlogfile\\.name=.*?\\s", outFile);
        }

        return launcherCmd;
    }

    /**
     * launch a worker in distributed mode
     *
     * @throws IOException
     */
    public void launchWorker(Map conf, IContext sharedContext, String topologyId, String supervisorId,
                             Integer port, String workerId, LocalAssignment assignment) throws IOException {
        // get supervisor conf
        Map stormConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
        String stormHome = System.getProperty("jstorm.home");
        if (StringUtils.isBlank(stormHome)) {
            stormHome = "./";
        }

        // get worker conf
        String topologyRoot = StormConfig.supervisor_stormdist_root(conf, topologyId);
        Map workerConf = StormConfig.read_topology_conf(topologyRoot, topologyId);

        Map totalConf = new HashMap();
        totalConf.putAll(conf);
        totalConf.putAll(stormConf);
        totalConf.putAll(workerConf);

        /**
         * don't use environment.putAll(System.getenv);
         */
        Map<String, String> environment = new HashMap<>();
        if (ConfigExtension.getWorkerRedirectOutput(totalConf)) {
            environment.put("REDIRECT", "true");
        } else {
            environment.put("REDIRECT", "false");
        }
        environment.put("LD_LIBRARY_PATH", (String) totalConf.get(Config.JAVA_LIBRARY_PATH));
        environment.put("jstorm.home", stormHome);
        environment.put("jstorm.workerId", workerId);
        environment.put("jstorm.on.yarn", isJstormOnYarn ? "1" : "0");

        String launcherCmd = getLauncherParameter(assignment, totalConf, stormHome, topologyId, port);
        String workerCmd = getWorkerParameter(assignment,
                totalConf,
                stormHome,
                topologyId,
                supervisorId,
                workerId,
                port);
        String cmd = launcherCmd + " " + workerCmd;
        cmd = cmd.replace("%JSTORM_HOME%", stormHome);

        LOG.info("Launching worker with command: " + cmd);
        LOG.info("Environment:" + environment.toString());

        /**
         * if run on yarn, set backend false, otherwise set true
         */
        boolean backend = !isJstormOnYarn;
        LOG.info("backend mode is " + backend);
        JStormUtils.launchProcess(cmd, environment, backend);
    }

    private Set<Integer> killUselessWorkers(Map<String, StateHeartbeat> localWorkerStats,
                                            Map<Integer, LocalAssignment> localAssignments,
                                            Map<String, Integer> taskCleanupTimeoutMap) {
        Map<String, String> removed = new HashMap<>();
        Set<Integer> keepPorts = new HashSet<>();

        for (Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {

            String workerId = entry.getKey();
            StateHeartbeat hbState = entry.getValue();
            if (workerIdToStartTimeAndPort.containsKey(workerId) && hbState.getState().equals(State.notStarted))
                continue;

            if (hbState.getState().equals(State.valid)) {
                // hbstate.getHeartbeat() won't be null
                keepPorts.add(hbState.getHeartbeat().getPort());
            } else {
                if (hbState.getHeartbeat() != null) {
                    removed.put(workerId, hbState.getHeartbeat().getTopologyId());
                } else {
                    removed.put(workerId, null);
                }

                if (!killingWorkers.containsKey(workerId)) {
                    LOG.info("Shutting down and clearing state for id {}, State:{}", workerId, hbState);
                }
            }
        }

        shutWorker(conf, supervisorId, removed, workerThreadPids, cgroupManager, false, killingWorkers, taskCleanupTimeoutMap);
        Set<String> activeTopologies = new HashSet<>();
        if (killingWorkers.size() == 0) {
            // When all workers under killing are killed successfully,
            // clean the task cleanup timeout map correspondingly.
            for (Entry<Integer, LocalAssignment> entry : localAssignments.entrySet()) {
                activeTopologies.add(entry.getValue().getTopologyId());
            }

            Set<String> obsoleteTopologies = new HashSet<>();
            for (String topologyId : taskCleanupTimeoutMap.keySet()) {
                if (!activeTopologies.contains(topologyId)) {
                    obsoleteTopologies.add(topologyId);
                }
            }
            for (String topologyId : obsoleteTopologies) {
                taskCleanupTimeoutMap.remove(topologyId);
            }
        }

        for (String removedWorkerId : removed.keySet()) {
            localWorkerStats.remove(removedWorkerId);
        }
        // Keep the workers which are still under starting
        for (Entry<String, Pair<Integer, Integer>> entry : workerIdToStartTimeAndPort.entrySet()) {
            String workerId = entry.getKey();
            StateHeartbeat hbState = localWorkerStats.get(workerId);
            if (hbState != null)
                if (hbState.getState().equals(State.notStarted)) {
                    keepPorts.add(entry.getValue().getSecond());
                }
        }
        return keepPorts;
    }

    private String getWorkerRedirectOutput(Map totalConf, LocalAssignment assignment, int port) {

        String DEFAULT_OUT_TARGET_FILE = JStormUtils.genLogName(assignment.getTopologyName(), port);
        if (DEFAULT_OUT_TARGET_FILE == null) {
            DEFAULT_OUT_TARGET_FILE = "/dev/null";
        } else {
            DEFAULT_OUT_TARGET_FILE += ".out";
        }
        String outputFile = ConfigExtension.getWorkerRedirectOutputFile(totalConf);
        if (outputFile == null) {
            outputFile = DEFAULT_OUT_TARGET_FILE;
        } else {
            outputFile = outputFile + "-" + assignment.getTopologyName() + "-" + port;
        }
        return outputFile;
    }

    private void startNewWorkers(Set<Integer> keepPorts, Map<Integer, LocalAssignment> localAssignments,
                                 Set<String> downloadFailedTopologyIds) throws Exception {
        /**
         * Step 4: get reassigned tasks, which is in assignedTasks, but not in keeperPorts Map<port(type Integer), LocalAssignment>
         */
        Map<Integer, LocalAssignment> newWorkers = JStormUtils.select_keys_pred(keepPorts, localAssignments);

        /**
         * Step 5: generate new work ids
         */

        for (Iterator<Entry<Integer, LocalAssignment>> entryItr = newWorkers.entrySet().iterator();
             entryItr.hasNext(); ) {
            Entry<Integer, LocalAssignment> entry = entryItr.next();
            Integer port = entry.getKey();
            LocalAssignment assignment = entry.getValue();
            if (assignment != null && downloadFailedTopologyIds.contains(assignment.getTopologyId())) {
                LOG.info("Can't start this worker: {} of topology: {} due to the damaged binary!!",
                        port, assignment.getTopologyId());
                entryItr.remove();
            }
        }

        startWorkers(newWorkers);
    }

    private void startWorkers(Map<Integer, LocalAssignment> portToAssignment) throws Exception {
        Map<Integer, String> newWorkerIds = new HashMap<>();

        for (Entry<Integer, LocalAssignment> entry : portToAssignment.entrySet()) {
            Integer port = entry.getKey();
            LocalAssignment assignment = entry.getValue();

            String workerId = UUID.randomUUID().toString();
            newWorkerIds.put(port, workerId);

            // create new worker Id directory
            // LOCALDIR/workers/newworkid/pids
            try {
                StormConfig.worker_pids_root(conf, workerId);
            } catch (IOException e1) {
                LOG.error("Failed to create local dir for worker:" + workerId, e1);
                throw e1;
            }

            LOG.info("Launching worker with assignment {} for the supervisor {} on port {} with worker id {}",
                    assignment, supervisorId, port, workerId);
            try {
                doLaunchWorker(assignment, port, workerId);
            } catch (Exception e) {
                workerReportError.report(assignment.getTopologyId(), port,
                        assignment.getTaskIds(), JStormUtils.getErrorInfo(e), ErrorConstants.CODE_WORKER_EX);
                LOG.error("Failed to launchWorker workerId:" + workerId + ":" + port, e);
                throw e;
            }
        }

        markAllNewWorkers(newWorkerIds);
    }

    private void doLaunchWorker(LocalAssignment assignment, Integer port, String workerId)
            throws Exception {
        String clusterMode = StormConfig.cluster_mode(conf);
        String topologyId = assignment.getTopologyId();
        if (clusterMode.equals("distributed")) {
            launchWorker(conf, sharedContext, topologyId, supervisorId, port, workerId, assignment);
        } else if (clusterMode.equals("local")) {
            launchWorker(conf, sharedContext, topologyId, supervisorId, port, workerId, workerThreadPids);
        }
    }

    boolean isWorkerDead(String workerId) {
        if (!ConfigExtension.isCheckWorkerAliveBySystemInfo(conf)) {
            return false;
        }

        try {
            List<String> pids = getPid(conf, workerId);
            if (pids == null || pids.size() == 0) {
                // local mode doesn't exist pid
                return false;
            }
            // if all pid in pids are dead, then the worker is dead
            for (String pid : pids) {
                boolean isDead = JStormUtils.isProcDead(pid);
                if (isDead) {
                    LOG.info("Found worker {} is dead ", workerId);
                } else {
                    return false;
                }
            }

            return true;
        } catch (IOException e) {
            LOG.info("Failed to check whether worker is dead through /proc/pid", e);
            return false;
        }
    }
}
