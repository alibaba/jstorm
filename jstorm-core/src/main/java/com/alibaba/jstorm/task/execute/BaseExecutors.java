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
import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TickTupleTrigger;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base executor, shared between spout and bolt
 *
 * @author Longda
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
    protected TaskBaseMetric taskStats;

    protected volatile TaskStatus taskStatus;

    protected int messageTimeoutSecs = 30;

    protected Throwable error = null;

    protected ITaskReportErr reportError;

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

    protected TickTupleTrigger tickTupleTrigger;

    // protected IntervalCheck intervalCheck = new IntervalCheck();

    protected boolean isBatchMode;

    public BaseExecutors(Task task) {
        this.task = task;
        this.storm_conf = task.getStormConf();

        this.userTopologyCtx = task.getUserContext();
        this.sysTopologyCtx = task.getTopologyContext();
        this.taskStats = task.getTaskStats();
        this.taskId = sysTopologyCtx.getThisTaskId();
        this.innerTaskTransfer = task.getInnerTaskTransfer();
        this.topologyId = sysTopologyCtx.getTopologyId();
        this.componentId = sysTopologyCtx.getThisComponentId();
        this.idStr = JStormServerUtils.getName(componentId, taskId);

        this.taskStatus = task.getTaskStatus();
        this.executorStatus = new TaskStatus();
        this.reportError = task.getReportErrorDie();
        this.taskTransfer = task.getTaskTransfer();
        this.metricsReporter = task.getWorkerData().getMetricsReporter();

        messageTimeoutSecs = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);
        long timeout = JStormUtils.parseLong(storm_conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        boolean isDisruptorBatchMode = ConfigExtension.isDisruptorQueueBatchMode(storm_conf);
        int disruptorBatch = ConfigExtension.getDisruptorBufferSize(storm_conf);
        long flushMs = ConfigExtension.getDisruptorBufferFlushMs(storm_conf);

        this.exeQueue = DisruptorQueue.mkInstance(idStr, ProducerType.MULTI, queue_size,
                waitStrategy, isDisruptorBatchMode, disruptorBatch, flushMs);
        //this.exeQueue.consumerStarted();
        queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_CTRL_BUFFER_SIZE), 32);
        this.controlQueue = DisruptorQueue.mkInstance(
                idStr + " for control message", ProducerType.MULTI, queue_size, waitStrategy, false, 0, 0);
        //this.controlQueue.consumerStarted();

        this.registerInnerTransfer(exeQueue);
        this.task.getControlQueues().put(taskId, this.controlQueue);

        QueueGauge exeQueueGauge = new QueueGauge(exeQueue, idStr, MetricDef.EXECUTE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                        topologyId, componentId, taskId, MetricDef.EXECUTE_QUEUE, MetricType.GAUGE),
                new AsmGauge(exeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.EXECUTE_QUEUE, exeQueueGauge);
        //metric for control queue
        QueueGauge controlQueueGauge = new QueueGauge(controlQueue, idStr, MetricDef.CONTROL_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                        topologyId, componentId, taskId, MetricDef.CONTROL_QUEUE, MetricType.GAUGE),
                new AsmGauge(controlQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.CONTROL_QUEUE, controlQueueGauge);

        rotatingMapTrigger = new RotatingMapTrigger(storm_conf, idStr + "_rotating", controlQueue);
        rotatingMapTrigger.register();
        taskHbTrigger = new TaskHeartbeatTrigger(storm_conf, idStr + "_taskHeartbeat", controlQueue,
                taskId, componentId, sysTopologyCtx, reportError, executorStatus);
        assignmentTs = System.currentTimeMillis();
        isBatchMode = ConfigExtension.isTaskBatchTuple(storm_conf);
    }

    public void init() throws Exception {
        // this function will be overridden by SpoutExecutor or BoltExecutor
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
            reportError.report(e);
        } finally {
            LOG.info("{} init finished", idStr);

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
            LOG.error("TaskStatus isn't shutdown, but calling shutdown, illegal state!");
        }
        if (this.rotatingMapTrigger != null) {
            this.rotatingMapTrigger.unregister();
        }
        if (this.tickTupleTrigger != null) {
            this.tickTupleTrigger.unregister();
        }
        this.unregistorInnerTransfer();
    }

    protected void registerInnerTransfer(DisruptorQueue disruptorQueue) {
        LOG.info("Register inner transfer for executor thread of " + idStr);
        DisruptorQueue existInnerTransfer = innerTaskTransfer.get(taskId);
        if (existInnerTransfer != null) {
            LOG.info("Inner task transfer exists already for executing thread of " + idStr);
            if (existInnerTransfer != disruptorQueue) {
                throw new RuntimeException("Inner task transfer must be unique in executing thread of " + idStr);
            }
        }
        innerTaskTransfer.put(taskId, disruptorQueue);
    }

    protected void unregistorInnerTransfer() {
        LOG.info("Unregister inner transfer for executor thread of " + idStr);
        innerTaskTransfer.remove(taskId);
    }

    protected int getMinTaskIdOfWorker() {
        SortedSet<Integer> tasks = new TreeSet<>(sysTopologyCtx.getThisWorkerTasks());
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
        WorkerSlot nodePort = task.getTaskToNodePort().get(taskId);
        if (nodePort != null) {
            conn = task.getNodePortToSocket().get(nodePort);
        }
        return conn;
    }

    @SuppressWarnings("unchecked")
    public void update(Map conf) {
        for (Object key : conf.keySet()) {
            storm_conf.put(key, conf.get(key));
        }
    }

    protected void consumeBatch(EventHandler<Object> handler) {
        boolean isConsumeEvent = false;
        if (controlQueue.population() > 0) {
            controlQueue.consumeBatch(handler);
            isConsumeEvent = true;
        }
        if (exeQueue.population() > 0) {
            exeQueue.consumeBatch(handler);
            isConsumeEvent = true;
        }
        if (!isConsumeEvent)
            JStormUtils.sleepMs(1);
    }
}
