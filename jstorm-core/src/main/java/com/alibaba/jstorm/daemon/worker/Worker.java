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
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.daemon.worker.hearbeat.WorkerHeartbeatRunable;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * worker entrance
 *
 * @author yannian/Longda
 */
public class Worker {

    private static Logger LOG = LoggerFactory.getLogger(Worker.class);

    /**
     * Why need workerData, it is for thread comeptition
     */
    private WorkerData workerData;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Worker(Map conf, IContext context, String topology_id, String supervisor_id, int port, String worker_id, String jar_path) throws Exception {
        workerData = new WorkerData(conf, context, topology_id, supervisor_id, port, worker_id, jar_path);
    }

    /**
     * get current task's output task list
     */
    public static Set<Integer> worker_output_tasks(WorkerData workerData) {
        ContextMaker context_maker = workerData.getContextMaker();
        Set<Integer> task_ids = workerData.getTaskids();
        StormTopology topology = workerData.getSysTopology();

        Set<Integer> rtn = new HashSet<Integer>();

        for (Integer taskid : task_ids) {
            TopologyContext context = context_maker.makeTopologyContext(topology, taskid, null);

            // <StreamId, <ComponentId, Grouping>>
            Map<String, Map<String, Grouping>> targets = context.getThisTargets();
            for (Map<String, Grouping> e : targets.values()) {
                for (String componentId : e.keySet()) {
                    List<Integer> tasks = context.getComponentTasks(componentId);
                    rtn.addAll(tasks);
                }
            }
        }

        return rtn;
    }

    private RefreshConnections makeRefreshConnections() {

        // get output streams of every task
        Set<Integer> outboundTasks = worker_output_tasks(workerData);

        workerData.initOutboundTaskStatus(outboundTasks);
        workerData.setOutboundTasks(outboundTasks);

        RefreshConnections refresh_connections = new RefreshConnections(workerData);

        return refresh_connections;
    }

    private List<TaskShutdownDameon> createTasks() throws Exception {
        List<TaskShutdownDameon> shutdowntasks =
                new ArrayList<TaskShutdownDameon>();

        Set<Integer> taskids = workerData.getTaskids();

        Set<Thread> threads = new HashSet<Thread>();
        List<Task> taskArrayList = new ArrayList<Task>();
        for (int taskid : taskids) {
            Task task = new Task(workerData, taskid);
            Thread thread = new Thread(task);
            threads.add(thread);
            taskArrayList.add(task);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (Task t : taskArrayList) {
            shutdowntasks.add(t.getTaskShutdownDameon());
        }
        return shutdowntasks;
    }

    private AsyncLoopThread startDispatchThread() {
        // send tuple directly from nettyserver
        // send control tuple to dispatch thread
        // startDispatchDisruptor();

        IContext context = workerData.getContext();
        String topologyId = workerData.getTopologyId();

        //create recv connection
        Map stormConf = workerData.getStormConf();
        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(stormConf);
        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 256);
        DisruptorQueue recvControlQueue = DisruptorQueue.mkInstance("Dispatch-control", ProducerType.MULTI,
                queueSize, waitStrategy);

        //metric for recvControlQueue
        QueueGauge revCtrlGauge = new QueueGauge(recvControlQueue, MetricDef.RECV_CTRL_QUEUE);
        JStormMetrics.registerWorkerMetric(JStormMetrics.workerMetricName(MetricDef.RECV_CTRL_QUEUE, MetricType.GAUGE), new AsmGauge(
                revCtrlGauge));

        IConnection recvConnection = context.bind(topologyId, workerData.getPort(), workerData.getDeserializeQueues(), recvControlQueue);
        workerData.setRecvConnection(recvConnection);

        // create recvice control messages's thread
        RunnableCallback recvControlDispather = null;

        boolean isTaskBatchTuple = ConfigExtension.isTaskBatchTuple(stormConf);
        if (isTaskBatchTuple) {
            recvControlDispather = new VirtualPortBatchCtrlDispatch(workerData, recvConnection, recvControlQueue, MetricDef.BATCH_RECV_THREAD);
        } else {
            recvControlDispather = new VirtualPortCtrlDispatch(workerData, recvConnection, recvControlQueue, MetricDef.RECV_THREAD);
        }

        AsyncLoopThread recvControlThread = new AsyncLoopThread(recvControlDispather, false,
                Thread.MAX_PRIORITY, true);
        return recvControlThread;
    }

    public WorkerShutdown execute() throws Exception {
        List<AsyncLoopThread> threads = new ArrayList<AsyncLoopThread>();

        // create recv connection
        AsyncLoopThread controlRvthread = startDispatchThread();
        threads.add(controlRvthread);

        // create client before create task
        // so create client connection before create task
        // refresh connection
        RefreshConnections refreshConn = makeRefreshConnections();
        AsyncLoopThread refreshconn = new AsyncLoopThread(refreshConn, false, Thread.MIN_PRIORITY, true);
        threads.add(refreshconn);

        // refresh ZK active status
        RefreshActive refreshZkActive = new RefreshActive(workerData);
        AsyncLoopThread refreshzk = new AsyncLoopThread(refreshZkActive, false, Thread.MIN_PRIORITY, true);
        threads.add(refreshzk);

        //create send control message thread
        DrainerCtrlRunable drainerCtrlRunable;
        boolean isTaskBatchTuple = ConfigExtension.isTaskBatchTuple(workerData.getStormConf());
        if (isTaskBatchTuple) {
            drainerCtrlRunable = new DrainerBatchCtrlRunable(workerData, MetricDef.BATCH_SEND_THREAD);
        } else {
            drainerCtrlRunable = new DrainerCtrlRunable(workerData, MetricDef.SEND_THREAD);
        }
        AsyncLoopThread controlSendThread = new AsyncLoopThread(drainerCtrlRunable, false, Thread.MAX_PRIORITY, true);
        threads.add(controlSendThread);

        // Sync heartbeat to Apsara Container
        AsyncLoopThread syncContainerHbThread = SyncContainerHb.mkWorkerInstance(workerData.getStormConf());
        if (syncContainerHbThread != null) {
            threads.add(syncContainerHbThread);
        }

        JStormMetricsReporter metricReporter = new JStormMetricsReporter(workerData);
        metricReporter.init();
        workerData.setMetricsReporter(metricReporter);

        // refresh hearbeat to Local dir
        RunnableCallback heartbeat_fn = new WorkerHeartbeatRunable(workerData);
        AsyncLoopThread hb = new AsyncLoopThread(heartbeat_fn, false, null, Thread.NORM_PRIORITY, true);
        threads.add(hb);

        // shutdown task callbacks
        List<TaskShutdownDameon> shutdowntasks = createTasks();
        workerData.setShutdownTasks(shutdowntasks);


        return new WorkerShutdown(workerData, threads);

    }

    /**
     * create worker instance and run it
     *
     * @param conf
     * @param topology_id
     * @param supervisor_id
     * @param port
     * @param worker_id
     * @return
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public static WorkerShutdown mk_worker(Map conf, IContext context, String topology_id, String supervisor_id, int port, String worker_id, String jar_path)
            throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("topologyId:" + topology_id + ", ");
        sb.append("port:" + port + ", ");
        sb.append("workerId:" + worker_id + ", ");
        sb.append("jarPath:" + jar_path + "\n");

        LOG.info("Begin to run worker:" + sb.toString());

        Worker w = new Worker(conf, context, topology_id, supervisor_id, port, worker_id, jar_path);

        //   w.redirectOutput();

        return w.execute();
    }

    public void redirectOutput() {

        if (System.getenv("REDIRECT") == null || !System.getenv("REDIRECT").equals("true")) {
            return;
        }

        String DEFAULT_OUT_TARGET_FILE = JStormUtils.getLogFileName();
        if (DEFAULT_OUT_TARGET_FILE == null) {
            DEFAULT_OUT_TARGET_FILE = "/dev/null";
        } else {
            DEFAULT_OUT_TARGET_FILE += ".out";
        }

        String outputFile = ConfigExtension.getWorkerRedirectOutputFile(workerData.getStormConf());
        if (outputFile == null) {
            outputFile = DEFAULT_OUT_TARGET_FILE;
        } else {
            try {
                File file = new File(outputFile);
                if (file.exists() == false) {
                    PathUtils.touch(outputFile);
                } else {
                    if (file.isDirectory() == true) {
                        LOG.warn("Failed to write " + outputFile);
                        outputFile = DEFAULT_OUT_TARGET_FILE;
                    } else if (file.canWrite() == false) {
                        LOG.warn("Failed to write " + outputFile);
                        outputFile = DEFAULT_OUT_TARGET_FILE;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to touch " + outputFile, e);
                outputFile = DEFAULT_OUT_TARGET_FILE;
            }
        }

        try {
            JStormUtils.redirectOutput(outputFile);
        } catch (Exception e) {
            LOG.warn("Failed to redirect to " + outputFile, e);
        }

    }

    /**
     * Have one problem if the worker's start parameter length is longer than 4096, ps -ef|grep com.alibaba.jstorm.daemon.worker.Worker can't find worker
     *
     * @param port
     */

    public static List<Integer> getOldPortPids(String port) {
        String currPid = JStormUtils.process_pid();

        List<Integer> ret = new ArrayList<Integer>();

        StringBuilder sb = new StringBuilder();

        sb.append("ps -Af ");
        // sb.append(" | grep ");
        // sb.append(Worker.class.getName());
        // sb.append(" |grep ");
        // sb.append(port);
        // sb.append(" |grep -v grep");

        try {
            LOG.info("Begin to execute " + sb.toString());
            String output = JStormUtils.launchProcess(sb.toString(), new HashMap<String, String>(), false);

            BufferedReader reader = new BufferedReader(new StringReader(output));

            JStormUtils.sleepMs(1000);

            // if (process.exitValue() != 0) {
            // LOG.info("Failed to execute " + sb.toString());
            // return null;
            // }

            String str;
            while ((str = reader.readLine()) != null) {
                if (StringUtils.isBlank(str)) {
                    // LOG.info(str + " is Blank");
                    continue;
                }

                // LOG.info("Output:" + str);
                if (str.contains(Worker.class.getName()) == false) {
                    continue;
                } else if (str.contains(port) == false) {
                    continue;
                }

                LOG.info("Find :" + str);

                String[] fields = StringUtils.split(str);

                boolean find = false;
                int i = 0;
                for (; i < fields.length; i++) {
                    String field = fields[i];
                    LOG.debug("Filed, " + i + ":" + field);

                    if (field.contains(Worker.class.getName()) == true) {
                        if (i + 3 >= fields.length) {
                            LOG.info("Failed to find port ");

                        } else if (fields[i + 3].equals(String.valueOf(port))) {
                            find = true;
                        }

                        break;
                    }
                }

                if (find == false) {
                    LOG.info("No old port worker");
                    continue;
                }

                if (fields.length >= 2) {
                    try {
                        if (currPid.equals(fields[1])) {
                            LOG.info("Skip kill myself");
                            continue;
                        }
                        Integer pid = Integer.valueOf(fields[1]);

                        LOG.info("Find one process :" + pid.toString());
                        ret.add(pid);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                        continue;
                    }
                }
            }
            return ret;
        } catch (IOException e) {
            LOG.info("Failed to execute " + sb.toString());
            return ret;
        } catch (Exception e) {
            LOG.info(e.getMessage(), e);
            return ret;
        }
    }

    public static void killOldWorker(String port) {
        List<Integer> oldPids = getOldPortPids(port);
        for (Integer pid : oldPids) {
            JStormUtils.kill(pid);
        }
    }

    /**
     * worker entrance
     *
     * @param args
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg + " ");
        }
        LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        LOG.info("Begin to start worker:" + sb.toString());
        LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        if (args.length < 5) {
            LOG.error("The length of args is less than 5 ");
            System.exit(-1);
        }

        sb = new StringBuilder();
        try {
            String topology_id = args[0];
            String supervisor_id = args[1];
            String port_str = args[2];
            String worker_id = args[3];
            String jar_path = args[4];

            killOldWorker(port_str);

            Map conf = Utils.readStormConfig();
            StormConfig.validate_distributed_mode(conf);

            JStormServerUtils.startTaobaoJvmMonitor();

            sb.append("topologyId:" + topology_id + ", ");
            sb.append("port:" + port_str + ", ");
            sb.append("workerId:" + worker_id + ", ");
            sb.append("jar_path:" + jar_path + "\n");

            WorkerShutdown sd = mk_worker(conf, null, topology_id, supervisor_id, Integer.parseInt(port_str), worker_id, jar_path);
            sd.join();

            LOG.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            LOG.info("Successfully shutdown worker " + sb.toString());
            LOG.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        } catch (Throwable e) {
            String errMsg = "Failed to create worker, " + sb.toString();
            LOG.error(errMsg, e);
            JStormUtils.halt_process(-1, errMsg);
        }
    }

}
