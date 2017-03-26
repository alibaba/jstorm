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
package com.alibaba.jstorm.daemon.supervisor;

import com.alibaba.jstorm.config.SupervisorRefreshConfig;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.jstorm.daemon.worker.WorkerReportError;
import com.alibaba.jstorm.utils.DefaultUncaughtExceptionHandler;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.event.EventManagerImp;
import com.alibaba.jstorm.event.EventManagerPusher;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Supervisor workflow
 *
 * 1. write SupervisorInfo to ZK
 *
 * 2. Every 10 seconds run SynchronizeSupervisor
 * 2.1 download new topology
 * 2.2 release useless worker
 * 2.3 assign new task to /local-dir/supervisor/localstate
 * 2.4 add one syncProcesses event
 *
 * 3. Every supervisor.monitor.frequency.secs run SyncProcesses
 * 3.1 kill useless worker
 * 3.2 start new worker
 *
 * 4. create heartbeat thread every supervisor.heartbeat.frequency.secs, write SupervisorInfo to ZK
 *
 * @author Johnfang (xiaojian.fxj@alibaba-inc.com)
 */

@SuppressWarnings({"unchecked", "unused"})
public class Supervisor {

    private static Logger LOG = LoggerFactory.getLogger(Supervisor.class);

    volatile MachineCheckStatus checkStatus = new MachineCheckStatus();
    //volatile HealthStatus healthStatus = new HealthStatus();

    /**
     * create and start a supervisor
     *
     * @param conf          : configuration (default.yaml & storm.yaml)
     * @param sharedContext : null (right now)
     * @return SupervisorManger: which is used to shutdown all workers and supervisor
     */
    @SuppressWarnings("rawtypes")
    public SupervisorManger mkSupervisor(Map conf, IContext sharedContext) throws Exception {

        LOG.info("Starting Supervisor with conf " + conf);

        /**
         * Step 1: cleanup all files in /storm-local-dir/supervisor/tmp
         */
        String path = StormConfig.supervisorTmpDir(conf);
        FileUtils.cleanDirectory(new File(path));

        /**
         * Step 2: create ZK operation instance StormClusterState
         */

        StormClusterState stormClusterState = Cluster.mk_storm_cluster_state(conf);

        String hostName = JStormServerUtils.getHostName(conf);
        WorkerReportError workerReportError = new WorkerReportError(stormClusterState, hostName);

        /**
         * Step 3, create LocalStat (a simple KV store)
         * 3.1 create LocalState instance;
         * 3.2 get supervisorId, if there's no supervisorId, create one
         */

        LocalState localState = StormConfig.supervisorState(conf);

        String supervisorId = (String) localState.get(Common.LS_ID);
        if (supervisorId == null) {
            supervisorId = UUID.randomUUID().toString();
            localState.put(Common.LS_ID, supervisorId);
        }
        //clean LocalStat's zk-assignment & versions
        localState.remove(Common.LS_LOCAl_ZK_ASSIGNMENTS);
        localState.remove(Common.LS_LOCAL_ZK_ASSIGNMENT_VERSION);

        Vector<AsyncLoopThread> threads = new Vector<>();

        // Step 4 create HeartBeat
        // every supervisor.heartbeat.frequency.secs, write SupervisorInfo to ZK
        // sync heartbeat to nimbus
        Heartbeat hb = new Heartbeat(conf, stormClusterState, supervisorId, localState, checkStatus);
        hb.update();

        AsyncLoopThread heartbeat = new AsyncLoopThread(hb, false, null, Thread.MIN_PRIORITY, true);
        threads.add(heartbeat);

        // Sync heartbeat to Apsara Container
        AsyncLoopThread syncContainerHbThread = SyncContainerHb.mkSupervisorInstance(conf);
        if (syncContainerHbThread != null) {
            threads.add(syncContainerHbThread);
        }

        // Step 5 create and start sync Supervisor thread
        // every supervisor.monitor.frequency.secs second run SyncSupervisor
        ConcurrentHashMap<String, String> workerThreadPids = new ConcurrentHashMap<>();
        SyncProcessEvent syncProcessEvent = new SyncProcessEvent(
                supervisorId, conf, localState, workerThreadPids, sharedContext, workerReportError);

        EventManagerImp syncSupEventManager = new EventManagerImp();
        AsyncLoopThread syncSupEventThread = new AsyncLoopThread(syncSupEventManager);
        threads.add(syncSupEventThread);

        SyncSupervisorEvent syncSupervisorEvent = new SyncSupervisorEvent(
                supervisorId, conf, syncSupEventManager, stormClusterState, localState, syncProcessEvent, hb);

        int syncFrequency = JStormUtils.parseInt(conf.get(Config.SUPERVISOR_MONITOR_FREQUENCY_SECS));
        EventManagerPusher syncSupervisorPusher = new EventManagerPusher(
                syncSupEventManager, syncSupervisorEvent, syncFrequency);
        AsyncLoopThread syncSupervisorThread = new AsyncLoopThread(syncSupervisorPusher);
        threads.add(syncSupervisorThread);

        // Step 6 start httpserver
        Httpserver httpserver = null;
        if (!StormConfig.local_mode(conf)) {
            int port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf);
            httpserver = new Httpserver(port, conf);
            httpserver.start();
        }

        //Step 7 check supervisor
        if (!StormConfig.local_mode(conf)) {
            if (ConfigExtension.isEnableCheckSupervisor(conf)) {
                SupervisorHealth supervisorHealth = new SupervisorHealth(conf, checkStatus, supervisorId);
                AsyncLoopThread healthThread = new AsyncLoopThread(supervisorHealth, false, null, Thread.MIN_PRIORITY, true);
                threads.add(healthThread);
            }

            // init refresh config thread
            AsyncLoopThread refreshConfigThread = new AsyncLoopThread(new SupervisorRefreshConfig(conf));
            threads.add(refreshConfigThread);
        }

        // create SupervisorManger which can shutdown all supervisor and workers
        return new SupervisorManger(
                conf, supervisorId, threads, syncSupEventManager, httpserver, stormClusterState, workerThreadPids);
    }

    /**
     * shutdown
     */
    public void killSupervisor(SupervisorManger supervisor) {
        supervisor.shutdown();
    }

    private void initShutdownHook(SupervisorManger supervisor) {
        Runtime.getRuntime().addShutdownHook(new Thread(supervisor));
        //JStormUtils.registerJStormSignalHandler();
    }

    private void createPid(Map conf) throws Exception {
        String pidDir = StormConfig.supervisorPids(conf);

        JStormServerUtils.createPid(pidDir);
    }

    /**
     * start supervisor
     */
    public void run() {
        SupervisorManger supervisorManager;
        try {
            Map<Object, Object> conf = Utils.readStormConfig();

            StormConfig.validate_distributed_mode(conf);

            createPid(conf);

            supervisorManager = mkSupervisor(conf, null);

            JStormUtils.redirectOutput("/dev/null");

            initShutdownHook(supervisorManager);

            while (!supervisorManager.isFinishShutdown()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }

        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                LOG.error("Halting due to Out Of Memory Error...");
            }
            LOG.error("Fail to run supervisor ", e);
            System.exit(1);
        } finally {
            LOG.info("Shutdown supervisor!!!");
        }

    }

    /**
     * start supervisor daemon
     */
    public static void main(String[] args) {

        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

        JStormServerUtils.startTaobaoJvmMonitor();

        Supervisor instance = new Supervisor();

        instance.run();

    }
}
