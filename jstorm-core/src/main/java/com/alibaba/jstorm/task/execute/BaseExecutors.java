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
package com.alibaba.jstorm.task.execute;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

//import com.alibaba.jstorm.message.zeroMq.IRecvConnection;

/**
 * Base executor share between spout and bolt
 *
 *
 * @author Longda
 *
 */
public class BaseExecutors extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(BaseExecutors.class);

    protected final String topologyId;
    protected final String componentId;
    protected final int taskId;
    protected final String idStr;

    protected Map storm_conf;

    protected TopologyContext userTopologyCtx;
    protected TopologyContext sysTopologyCtx;
    protected TaskBaseMetric task_stats;

    protected volatile TaskStatus taskStatus;

    protected int message_timeout_secs = 30;

    protected Throwable error = null;

    protected ITaskReportErr report_error;

    protected DisruptorQueue exeQueue;
    protected DisruptorQueue controlQueue;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;

    protected Task task;
    protected long assignmentTs;
    protected TaskTransfer taskTransfer;

    protected JStormMetricsReporter metricsReporter;

    protected boolean isFinishInit = false;

    protected RotatingMapTrigger rotatingMapTrigger;
    protected TaskHeartbeatTrigger taskHbTrigger;
    protected TaskStatus executorStatus;

    // protected IntervalCheck intervalCheck = new IntervalCheck();

    protected boolean isBatchMode;

    public BaseExecutors(Task task) {

        this.task = task;
        this.storm_conf = task.getStormConf();

        this.userTopologyCtx = task.getUserContext();
        this.sysTopologyCtx = task.getTopologyContext();
        this.task_stats = task.getTaskStats();
        this.taskId = sysTopologyCtx.getThisTaskId();
        this.innerTaskTransfer = task.getInnerTaskTransfer();
        this.topologyId = sysTopologyCtx.getTopologyId();
        this.componentId = sysTopologyCtx.getThisComponentId();
        this.idStr = JStormServerUtils.getName(componentId, taskId);

        this.taskStatus = task.getTaskStatus();
        this.executorStatus = new TaskStatus();
        this.report_error = task.getReportErrorDie();
        this.taskTransfer = task.getTaskTransfer();
        this.metricsReporter = task.getWorkerData().getMetricsReporter();

        message_timeout_secs = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);
        long timeout = JStormUtils.parseLong(storm_conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        boolean isDisruptorBatchMode = ConfigExtension.isDisruptorQueueBatchMode(storm_conf);
        int disruptorBatch = ConfigExtension.getDisruptorBufferSize(storm_conf);
        long flushMs = ConfigExtension.getDisruptorBufferFlushMs(storm_conf);
        this.exeQueue = DisruptorQueue.mkInstance(idStr, ProducerType.MULTI, queue_size, waitStrategy, isDisruptorBatchMode,
                disruptorBatch, flushMs);
        //this.exeQueue.consumerStarted();
        queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 32);
        this.controlQueue = DisruptorQueue.mkInstance(idStr + " for control message", ProducerType.MULTI, queue_size, waitStrategy, false, 0, 0);
        //this.controlQueue.consumerStarted();

        this.registerInnerTransfer(exeQueue);
        this.task.getControlQueues().put(taskId, this.controlQueue);

        QueueGauge exeQueueGauge = new QueueGauge(exeQueue, idStr, MetricDef.EXECUTE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, componentId, taskId, MetricDef.EXECUTE_QUEUE, MetricType.GAUGE), new AsmGauge(
                exeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.EXECUTE_QUEUE, exeQueueGauge);
        //metric for control queue
        QueueGauge controlQueueGauge = new QueueGauge(controlQueue, idStr, MetricDef.CONTROL_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, componentId, taskId, MetricDef.CONTROL_QUEUE, MetricType.GAUGE), new AsmGauge(
                controlQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.CONTROL_QUEUE, controlQueueGauge);

        rotatingMapTrigger = new RotatingMapTrigger(storm_conf, idStr + "_rotating", controlQueue);
        rotatingMapTrigger.register();
        taskHbTrigger = new TaskHeartbeatTrigger(storm_conf, idStr + "_taskHeartbeat", controlQueue, taskId, componentId, sysTopologyCtx, report_error, executorStatus);
        assignmentTs = System.currentTimeMillis();
        isBatchMode = ConfigExtension.isTaskBatchTuple(storm_conf);
    }

    public void init() throws Exception {
        // this function will be override by SpoutExecutor or BoltExecutor
        throw new RuntimeException("Should implement this function");
    }

    public void initWrapper() {
        try {
            LOG.info("{} begin to init", idStr);

            init();

            if (taskId == getMinTaskIdOfWorker()) {
                metricsReporter.setOutputCollector(getOutputCollector());
            }

            isFinishInit = true;
        } catch (Throwable e) {
            error = e;
            LOG.error("Init error ", e);
            report_error.report(e);
        } finally {

            LOG.info("{} initialization finished", idStr);

        }
    }

    @Override
    public void preRun() {
        WorkerClassLoader.switchThreadContext();
    }

    @Override
    public void postRun() {
        WorkerClassLoader.restoreThreadContext();
    }

    @Override
    public void run() {
        // this function will be override by SpoutExecutor or BoltExecutor
        throw new RuntimeException("Should implement this function");
    }

    @Override
    public Exception error() {
        if (error == null) {
            return null;
        }
        return new Exception(error);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutdown executing thread of " + idStr);
        if (!taskStatus.isShutdown()) {
            LOG.error("Taskstatus isn't shutdown, but enter shutdown method, Occur exception");
        }
        this.unregistorInnerTransfer();

    }

    protected void registerInnerTransfer(DisruptorQueue disruptorQueue) {
        LOG.info("Registor inner transfer for executor thread of " + idStr);
        DisruptorQueue existInnerTransfer = innerTaskTransfer.get(taskId);
        if (existInnerTransfer != null) {
            LOG.info("Exist inner task transfer for executing thread of " + idStr);
            if (existInnerTransfer != disruptorQueue) {
                throw new RuntimeException("Inner task transfer must be only one in executing thread of " + idStr);
            }
        }
        innerTaskTransfer.put(taskId, disruptorQueue);
    }

    protected void unregistorInnerTransfer() {
        LOG.info("Unregistor inner transfer for executor thread of " + idStr);
        innerTaskTransfer.remove(taskId);
    }

    protected int getMinTaskIdOfWorker() {
        SortedSet<Integer> tasks = new TreeSet<Integer>(sysTopologyCtx.getThisWorkerTasks());
        return tasks.first();
    }

    public Object getOutputCollector() {
        return null;
    }

    public TaskHeartbeatTrigger getTaskHbTrigger() {
        return taskHbTrigger;
    }

    protected IConnection getConnection(int taskId) {
        IConnection conn = null;
        WorkerSlot nodePort = task.getTaskNodeport().get(taskId);
        if (nodePort != null) {
            conn = task.getNodeportSocket().get(nodePort);
        }
        return conn;
    }

    public void update(Map conf) {
        for (Object key : conf.keySet()) {
            storm_conf.put(key, conf.get(key));
        }
    }
}
