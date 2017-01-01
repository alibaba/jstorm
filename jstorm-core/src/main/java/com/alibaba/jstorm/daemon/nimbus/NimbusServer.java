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
package com.alibaba.jstorm.daemon.nimbus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.config.RefreshableComponents;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.supervisor.Httpserver;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.schedule.CleanRunnable;
import com.alibaba.jstorm.schedule.FollowerRunnable;
import com.alibaba.jstorm.schedule.MonitorRunnable;
import com.alibaba.jstorm.utils.DefaultUncaughtExceptionHandler;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.Utils;

/**
 * NimbusServer work flow: 1. cleanup interrupted topology delete /storm-local-dir/nimbus/topologyid/stormdis delete /storm-zk-root/storms/topologyid
 *
 * 2. set /storm-zk-root/storms/topology stats as run
 *
 * 3. start one thread, every nimbus.monitor.reeq.secs set /storm-zk-root/storms/ all topology as monitor. when the topology's status is monitor, nimubs would
 * reassign workers 4. start one threa, every nimubs.cleanup.inbox.freq.secs cleanup useless jar
 *
 * @author version 1: Nathan Marz version 2: Lixin/Chenjun version 3: Longda
 */
public class NimbusServer {

    private static final Logger LOG = LoggerFactory.getLogger(NimbusServer.class);

    private NimbusData data;

    private ServiceHandler serviceHandler;

    private TopologyAssign topologyAssign;

    private THsHaServer thriftServer;

    private FollowerRunnable follower;

    private Httpserver hs;

    private List<AsyncLoopThread> smartThreads = new ArrayList<AsyncLoopThread>();

    public static void main(String[] args) throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

        // read configuration files
        @SuppressWarnings("rawtypes")
        Map config = Utils.readStormConfig();

        JStormServerUtils.startTaobaoJvmMonitor();

        NimbusServer instance = new NimbusServer();

        INimbus iNimbus = new DefaultInimbus();

        instance.launchServer(config, iNimbus);

    }

    private void createPid(Map conf) throws Exception {
        String pidDir = StormConfig.masterPids(conf);

        JStormServerUtils.createPid(pidDir);
    }

    @SuppressWarnings("rawtypes")
    private void launchServer(final Map conf, INimbus inimbus) {
        LOG.info("Begin to start nimbus with conf " + conf);

        try {
            // 1. check whether mode is distributed or not
            StormConfig.validate_distributed_mode(conf);

            createPid(conf);

            initShutdownHook();

            inimbus.prepare(conf, StormConfig.masterInimbus(conf));

            data = createNimbusData(conf, inimbus);

            initFollowerThread(conf);

            int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
            hs = new Httpserver(port, conf);
            hs.start();

            initContainerHBThread(conf);

            serviceHandler = new ServiceHandler(data);
            initThrift(conf);
        } catch (Throwable e) {
            if (e instanceof OutOfMemoryError) {
                LOG.error("Halting due to Out Of Memory Error...");
            }
            LOG.error("Fail to run nimbus ", e);
        } finally {
            cleanup();
        }

        LOG.info("Quit nimbus");
    }

    /**
     * handle manual conf changes, check every 15 sec
     */
    private void mkRefreshConfThread(final NimbusData nimbusData) {
        nimbusData.getScheduExec().scheduleAtFixedRate(new RunnableCallback() {
            @Override
            public void run() {
                LOG.debug("checking changes in storm.yaml...");

                Map newConf = Utils.readStormConfig();
                if (Utils.isConfigChanged(nimbusData.getConf(), newConf)) {
                    LOG.warn("detected changes in storm.yaml, updating...");
                    synchronized (nimbusData.getConf()) {
                        nimbusData.getConf().clear();
                        nimbusData.getConf().putAll(newConf);
                    }

                    RefreshableComponents.refresh(newConf);
                } else {
                    LOG.debug("no changes detected, stay put.");
                }
            }

            @Override
            public Object getResult() {
                return 15;
            }
        }, 15, 15, TimeUnit.SECONDS);

        LOG.info("Successfully init configuration refresh thread");
    }

    public ServiceHandler launcherLocalServer(final Map conf, INimbus inimbus) throws Exception {
        LOG.info("Begin to start nimbus on local model");

        StormConfig.validate_local_mode(conf);

        inimbus.prepare(conf, StormConfig.masterInimbus(conf));

        data = createNimbusData(conf, inimbus);

        init(conf);

        serviceHandler = new ServiceHandler(data);
        return serviceHandler;
    }

    private void initContainerHBThread(Map conf) throws IOException {
        AsyncLoopThread thread = SyncContainerHb.mkNimbusInstance(conf);
        if (thread != null) {
            smartThreads.add(thread);
        }
    }

    private void initMetricRunnable() {
        AsyncLoopThread thread = new AsyncLoopThread(ClusterMetricsRunnable.getInstance());
        smartThreads.add(thread);
    }

    private void init(Map conf) throws Exception {

        data.init();

        NimbusUtils.cleanupCorruptTopologies(data);

        initTopologyAssign();

        initTopologyStatus();

        initCleaner(conf);

        initMetricRunnable();

        if (!data.isLocalMode()) {
            initMonitor(conf);
            //mkRefreshConfThread(data);
        }
    }

    @SuppressWarnings("rawtypes")
    private NimbusData createNimbusData(Map conf, INimbus inimbus) throws Exception {

        // Callback callback=new TimerCallBack();
        // StormTimer timer=Timer.mkTimerTimer(callback);
        return new NimbusData(conf, inimbus);
    }

    private void initTopologyAssign() {
        topologyAssign = TopologyAssign.getInstance();
        topologyAssign.init(data);
    }

    private void initTopologyStatus() throws Exception {
        // get active topology in ZK
        List<String> active_ids = data.getStormClusterState().active_storms();

        if (active_ids != null) {

            for (String topologyid : active_ids) {
                // set the topology status as startup
                // in fact, startup won't change anything
                NimbusUtils.transition(data, topologyid, false, StatusType.startup);
                NimbusUtils.updateTopologyTaskTimeout(data, topologyid);
                NimbusUtils.updateTopologyTaskHb(data, topologyid);
            }

        }

        LOG.info("Successfully init topology status");
    }

    @SuppressWarnings("rawtypes")
    private void initMonitor(Map conf) {
        final ScheduledExecutorService scheduExec = data.getScheduExec();

        if (!data.isLaunchedMonitor()) {
            // Schedule Nimbus monitor
            MonitorRunnable r1 = new MonitorRunnable(data);

            int monitor_freq_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
            scheduExec.scheduleAtFixedRate(r1, 0, monitor_freq_secs, TimeUnit.SECONDS);

            data.setLaunchedMonitor(true);
            LOG.info("Successfully init Monitor thread");
        } else {
            LOG.info("We have launched Monitor thread before");
        }
    }

    /**
     * Right now, every 600 seconds, nimbus will clean jar under /LOCAL-DIR/nimbus/inbox, which is the uploading topology directory
     *
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    private void initCleaner(Map conf) throws IOException {
        final ScheduledExecutorService scheduExec = data.getScheduExec();

        if (!data.isLaunchedCleaner()) {
            // Schedule Nimbus inbox cleaner/nimbus/inbox jar
            String dir_location = StormConfig.masterInbox(conf);
            int inbox_jar_expiration_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
            CleanRunnable r2 = new CleanRunnable(dir_location, inbox_jar_expiration_secs);

            int cleanup_inbox_freq_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);

            scheduExec.scheduleAtFixedRate(r2, 0, cleanup_inbox_freq_secs, TimeUnit.SECONDS);
            data.setLaunchedCleaner(true);
            LOG.info("Successfully init " + dir_location + " cleaner");
        } else {
            LOG.info("We have launched Cleaner thread before");
        }
    }

    @SuppressWarnings("rawtypes")
    private void initThrift(Map conf) throws TTransportException {
        Integer thrift_port = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT));
        TNonblockingServerSocket socket = new TNonblockingServerSocket(thrift_port);

        Integer maxReadBufSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE));

        THsHaServer.Args args = new THsHaServer.Args(socket);
        args.workerThreads(ServiceHandler.THREAD_NUM);
        args.protocolFactory(new TBinaryProtocol.Factory(false, true, maxReadBufSize, -1));

        args.processor(new Nimbus.Processor<Iface>(serviceHandler));
        args.maxReadBufferBytes = maxReadBufSize;

        thriftServer = new THsHaServer(args);

        LOG.info("Successfully started nimbus: started Thrift server...");
        thriftServer.serve();
    }

    private void initFollowerThread(Map conf) {
        // when this nimbus become leader, we will execute this callback, to init some necessary data/thread
        Callback leaderCallback = new Callback() {
            public <T> Object execute(T... args) {
                try {
                    init(data.getConf());
                } catch (Exception e) {
                    LOG.error("Nimbus init error after becoming a leader", e);
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
        follower = new FollowerRunnable(data, 5000, leaderCallback);
        Thread thread = new Thread(follower);
        thread.setDaemon(true);
        thread.start();
        LOG.info("Successfully init Follower thread");
    }

    private void initShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                NimbusServer.this.cleanup();
            }

        });

        //JStormUtils.registerJStormSignalHandler();
    }

    public void cleanup() {
        if (data == null || data.getIsShutdown().getAndSet(true)) {
            LOG.info("Notify to quit nimbus");
            return;
        }

        LOG.info("Begin to shutdown nimbus");
        AsyncLoopRunnable.getShutdown().set(true);

        data.getScheduExec().shutdownNow();

        for (AsyncLoopThread t : smartThreads) {

            t.cleanup();
            JStormUtils.sleepMs(10);
            t.interrupt();
            // try {
            // t.join();
            // } catch (InterruptedException e) {
            // LOG.error("join thread", e);
            // }
            LOG.info("Successfully cleanup " + t.getThread().getName());
        }

        if (serviceHandler != null) {
            serviceHandler.shutdown();
        }

        if (topologyAssign != null) {
            topologyAssign.cleanup();
            LOG.info("Successfully shutdown TopologyAssign thread");
        }

        if (follower != null) {
            follower.clean();
            LOG.info("Successfully shutdown follower thread");
        }

        if (data != null) {
            data.cleanup();
            LOG.info("Successfully shutdown NimbusData");
        }

        if (thriftServer != null) {
            thriftServer.stop();
            LOG.info("Successfully shutdown thrift server");
        }

        if (hs != null) {
            hs.shutdown();
            LOG.info("Successfully shutdown httpserver");
        }

        LOG.info("Successfully shutdown nimbus");
        // make sure shutdown nimbus
        JStormUtils.halt_process(0, "!!!Shutdown!!!");

    }

    public void uploadMetrics(String topologyId, TopologyMetric topologyMetric) throws TException {
        this.serviceHandler.uploadTopologyMetrics(topologyId, topologyMetric);
    }

    public Map<String, Long> registerMetrics(String topologyId, Set<String> names) throws TException {
        return this.serviceHandler.registerMetrics(topologyId, names);
    }
}
