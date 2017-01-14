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
package com.alibaba.jstorm.daemon.worker;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Shutdown worker
 *
 * @author yannian/Longda
 */
public class WorkerShutdown implements ShutdownableDameon {
    private static Logger LOG = LoggerFactory.getLogger(WorkerShutdown.class);

    private List<TaskShutdownDameon> shutdownTasks;
    private AtomicBoolean shutdown;
    private ConcurrentHashMap<WorkerSlot, IConnection> nodePortToSocket;
    private IContext context;
    private List<AsyncLoopThread> threads;
    private StormClusterState zkCluster;
    private ClusterState cluster_state;
    private FlusherPool flusherPool;
    private ScheduledExecutorService threadPool;
    private IConnection recvConnection;
    private Map conf;

    // active nodePortToSocket context zkCluster zkClusterstate
    public WorkerShutdown(WorkerData workerData, List<AsyncLoopThread> threads) {

        this.shutdownTasks = workerData.getShutdownTasks();
        this.threads = threads;

        this.shutdown = workerData.getShutdown();
        this.nodePortToSocket = workerData.getNodeportSocket();
        this.context = workerData.getContext();
        this.zkCluster = workerData.getZkCluster();
        this.threadPool = workerData.getThreadPool();
        this.cluster_state = workerData.getZkClusterstate();
        this.flusherPool = workerData.getFlusherPool();
        this.recvConnection = workerData.getRecvConnection();
        this.conf = workerData.getStormConf();

        Runtime.getRuntime().addShutdownHook(new Thread(this));

        // PreCleanupTasks preCleanupTasks = new PreCleanupTasks();
        // // install signals
        // Signal sig = new Signal(HOOK_SIGNAL);
        // Signal.handle(sig, preCleanupTasks);
    }

    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true)) {
            LOG.info("Worker has been shutdown already");
            return;
        }

        //dump worker jstack,jmap info to specific file
        if (ConfigExtension.isOutworkerDump(conf))
            workerDumpInfoOutput();

        // shutdown tasks
        List<Future<?>> futures = new ArrayList<>();
        for (ShutdownableDameon task : shutdownTasks) {
            Future<?> future = flusherPool.submit(task);
            futures.add(future);
        }
        // To be assure all tasks are closed rightly
        JStormServerUtils.checkFutures(futures);

        if (recvConnection != null) {
            recvConnection.close();
        }
        AsyncLoopRunnable.getShutdown().set(true);
        threadPool.shutdown();
        flusherPool.shutdown();

        try {
            flusherPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }

        // shutdown worker's demon thread
        // refreshconn, refreshzk, hb, drainer
        for (AsyncLoopThread t : threads) {
            LOG.info("Begin to shutdown " + t.getThread().getName());
            t.cleanup();
            JStormUtils.sleepMs(100);
            t.interrupt();
            // try {
            // t.join();
            // } catch (InterruptedException e) {
            // LOG.error("join thread", e);
            // }
            LOG.info("Successfully " + t.getThread().getName());
        }

        // send data to close connection
        for (WorkerSlot k : nodePortToSocket.keySet()) {
            IConnection value = nodePortToSocket.get(k);
            value.close();
        }

        context.term();

        // close ZK client
        try {
            zkCluster.disconnect();
            cluster_state.close();
        } catch (Exception e) {
            LOG.info("Shutdown error,", e);
        }

        JStormUtils.halt_process(0, "!!!Shutdown!!!");
    }

    public void join() throws InterruptedException {
        for (TaskShutdownDameon task : shutdownTasks) {
            task.join();
        }
        for (AsyncLoopThread t : threads) {
            t.join();
        }

    }

    public boolean waiting() {
        Boolean isExistsWait = false;
        for (ShutdownableDameon task : shutdownTasks) {
            if (task.waiting()) {
                isExistsWait = true;
                break;
            }
        }
        for (AsyncLoopThread thr : threads) {
            if (thr.isSleeping()) {
                isExistsWait = true;
                break;
            }
        }
        return isExistsWait;
    }

    @Override
    public void run() {
        shutdown();
    }

    private void workerDumpInfoOutput() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String pid = runtimeMXBean.getName().split("@")[0];
        LOG.debug("worker's pid is " + pid);

        String dumpOutFile = JStormUtils.getLogFileName();
        if (dumpOutFile == null) {
            return;
        } else {
            dumpOutFile += ".dump";
        }
        try {
            File file = new File(dumpOutFile);
            if (!file.exists()) {
                PathUtils.touch(dumpOutFile);
            }
        } catch (Exception e) {
            LOG.warn("Failed to touch " + dumpOutFile, e);
            return;
        }
        try {
            PrintWriter outFile = new PrintWriter(new FileWriter(dumpOutFile, true));
            StringBuilder jstackCommand = new StringBuilder();
            jstackCommand.append("jstack ");
            jstackCommand.append(pid);
            LOG.debug("Begin to execute " + jstackCommand.toString());
            String jstackOutput = JStormUtils.launchProcess(jstackCommand.toString(),
                    new HashMap<String, String>(), false);
            outFile.println(jstackOutput);


            StringBuilder jmapCommand = new StringBuilder();
            jmapCommand.append("jmap -heap ");
            jmapCommand.append(pid);
            LOG.debug("Begin to execute " + jmapCommand.toString());
            String jmapOutput = JStormUtils.launchProcess(jmapCommand.toString(),
                    new HashMap<String, String>(), false);
            outFile.println(jmapOutput);
        } catch (Exception e) {
            LOG.error("can't excute jstack and jmap about " + pid);
            LOG.error(String.valueOf(e));
        }
    }

}
