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
package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.task.GeneralTopologyContext;

import com.alibaba.jstorm.client.ConfigExtension;
import com.lmax.disruptor.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.dsl.ProducerType;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

/**
 * Sending entrance
 * <p/>
 * Task sending all tuples through this Object
 * <p/>
 * Serialize the Tuple and put the serialized data to the sending queue
 *
 * @author yannian
 */
public class TaskTransfer {

    private static Logger LOG = LoggerFactory.getLogger(TaskTransfer.class);

    protected Map stormConf;
    protected DisruptorQueue transferControlQueue;
/*    protected KryoTupleSerializer serializer;*/
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;
    protected Map<Integer, DisruptorQueue> controlQueues;
    protected DisruptorQueue serializeQueue;
    protected volatile TaskStatus taskStatus;
    protected String taskName;
    protected AsmHistogram serializeTimer;
    protected Task task;
    protected String topolgyId;
    protected String componentId;
    protected int taskId;

    protected ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    protected ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    protected boolean isTopologyMaster;

    protected boolean isBackpressureEnable;
    protected float highMark;
    protected float lowMark;
    protected Map<Integer, Boolean> targetTaskBackpressureStatus;

    protected int serializeThreadNum;
    protected final List<AsyncLoopThread> serializeThreads;

    protected final GeneralTopologyContext topologyContext;

    

    public TaskTransfer(Task task, String taskName, KryoTupleSerializer serializer, TaskStatus taskStatus, WorkerData workerData,
                        final GeneralTopologyContext context) {
        this.task = task;
        this.taskName = taskName;
/*        this.serializer = serializer;*/
        this.taskStatus = taskStatus;
        this.stormConf = task.getStormConf();
        this.transferControlQueue = workerData.getTransferCtrlQueue();
        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.controlQueues = workerData.getControlQueues();

        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();

        this.topolgyId = workerData.getTopologyId();
        this.componentId = this.task.getComponentId();
        this.taskId = this.task.getTaskId();
        this.topologyContext = context;

        int queue_size = Utils.getInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        long timeout = JStormUtils.parseLong(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        boolean isDisruptorBatchMode = ConfigExtension.isDisruptorQueueBatchMode(stormConf);
        int disruptorBatch = ConfigExtension.getDisruptorBufferSize(stormConf);
        long flushMs = ConfigExtension.getDisruptorBufferFlushMs(stormConf);
        this.serializeQueue = DisruptorQueue.mkInstance(taskName, ProducerType.MULTI, queue_size, waitStrategy, 
                isDisruptorBatchMode, disruptorBatch, flushMs);
        //this.serializeQueue.consumerStarted();

        serializeThreadNum = ConfigExtension.getTaskSerializeThreadNum(workerData.getStormConf());
        serializeThreads = new ArrayList<AsyncLoopThread>();
        setupSerializeThread();

        String taskId = taskName.substring(taskName.indexOf(":") + 1);
        QueueGauge serializeQueueGauge = new QueueGauge(serializeQueue, taskName, MetricDef.SERIALIZE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_QUEUE, MetricType.GAUGE),
                new AsmGauge(serializeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(Integer.valueOf(taskId), MetricDef.SERIALIZE_QUEUE, serializeQueueGauge);
        AsmHistogram serializeTimerHistogram = new AsmHistogram();
        serializeTimerHistogram.setAggregate(false);
        serializeTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_TIME, MetricType.HISTOGRAM), serializeTimerHistogram);

        isTopologyMaster = (task.getTopologyContext().getTopologyMasterId() == task.getTaskId());

        this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        this.highMark = (float) ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        this.lowMark = (float) ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        this.targetTaskBackpressureStatus = new ConcurrentHashMap<Integer, Boolean>();
        for (Integer innerTaskId : innerTaskTransfer.keySet()) {
            targetTaskBackpressureStatus.put(innerTaskId, false);
        }
        targetTaskBackpressureStatus.put(0, false); // for serialize task

        LOG.info("Successfully start TaskTransfer thread");
    }

    protected void setupSerializeThread() {
        for (int i = 0; i < serializeThreadNum; i++) {
            serializeThreads.add(new AsyncLoopThread(new TransferRunnable(i)));
        }
    }

    public void transfer(TupleExt tuple) {
        int taskId = tuple.getTargetTaskId();

        DisruptorQueue exeQueue = innerTaskTransfer.get(taskId);
        DisruptorQueue targetQueue;
        if (exeQueue == null) {
            taskId = 0;
            targetQueue = serializeQueue;
        } else {
            targetQueue = exeQueue;
        }

        if (isBackpressureEnable) {
            Boolean backpressureStatus = targetTaskBackpressureStatus.get(taskId);
            if (backpressureStatus == null) {
                backpressureStatus = false;
                targetTaskBackpressureStatus.put(taskId, backpressureStatus);
            }

            if (backpressureStatus) {
                while (targetQueue.pctFull() > lowMark) {
                    JStormUtils.sleepMs(1);
                }
                targetTaskBackpressureStatus.put(taskId, false);
                targetQueue.publish(tuple);
            } else  {
                targetQueue.publish(tuple);
                if (targetQueue.pctFull() > highMark) {
                    targetTaskBackpressureStatus.put(taskId, true);
                }
            } 
        } else {
            targetQueue.publish(tuple);
        }

    }

    public void transferControl(TupleExt tuple) {

        int taskId = tuple.getTargetTaskId();

        DisruptorQueue controlQueue = controlQueues.get(taskId);
        if (controlQueue != null) {
            controlQueue.publish(tuple);
        } else {
            transferControlQueue.publish(tuple);
        }
    }

    public void push(int taskId, TupleExt tuple) {
        serializeQueue.publish(tuple);
    }

    public List<AsyncLoopThread> getSerializeThreads() {
        return serializeThreads;
    }

    protected class TransferRunnable extends RunnableCallback implements EventHandler {

        private AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();
        private int threadIndex;
        protected KryoTupleSerializer serializer;

        public TransferRunnable(int threadIndex) {
            this.threadIndex = threadIndex;
            this.serializer = new KryoTupleSerializer(stormConf, topologyContext.getRawTopology());
        }

        @Override
        public String getThreadName() {
            return taskName + "-" + TransferRunnable.class.getSimpleName() + "-" + threadIndex;
        }


        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                serializeQueue.multiConsumeBatchWhenAvailable(this);
            }
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
            if (event == null) {
                return;
            }
            serialize(serializer, event);
        }
    }

    public void serializer(KryoTupleSerializer serializer) {
        LOG.debug("start Serializer of task, {}", taskId);
        if (!AsyncLoopRunnable.getShutdown().get()) {
            //note: avoid to cpu idle when serializeQueue is empty
            if (serializeQueue.population() == 0){
                Utils.sleep(1);
                return;
            }
            try {
                List<Object> objects = serializeQueue.retreiveAvailableBatch();
                for (Object object : objects) {
                    if (object == null) {
                        continue;
                    }
                    serialize(serializer, object);
                }
            } catch (InterruptedException e) {
                LOG.error("InterruptedException " + e.getCause());
                return;
            } catch (TimeoutException e) {
                return;
            }catch (AlertException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    protected IConnection getConnection(int taskId) {
        IConnection conn = null;
        WorkerSlot nodePort = taskNodeport.get(taskId);
        if (nodePort == null) {
            String errormsg = "IConnection to task-" + taskId + " can't be found";
            LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
        } else {
            conn = nodeportSocket.get(nodePort);
            if (conn == null) {
                String errormsg = "NodePort to" + nodePort + " can't be found";
                LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
            }
        }
        return conn;
    }

    protected void serialize(KryoTupleSerializer serializer, Object event){
        long start = serializeTimer.getTime();
        try {
            ITupleExt tuple = (ITupleExt) event;
            int targetTaskid = tuple.getTargetTaskId();
            IConnection conn = getConnection(targetTaskid);
            if (conn != null) {
                byte[] tupleMessage = serializer.serialize((TupleExt) tuple);
                //LOG.info("Task-{} sent msg to task-{}, data={}", task.getTaskId(), taskid, JStormUtils.toPrintableString(tupleMessage));
                TaskMessage taskMessage = new TaskMessage(targetTaskid, tupleMessage);
                conn.send(taskMessage);
            } else {
                LOG.error("Can not find connection for task-{}", targetTaskid);
            }
        } finally {
            if (MetricUtils.metricAccurateCal)
                serializeTimer.updateTime(start);
        }
    }
}
