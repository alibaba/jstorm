/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import backtype.storm.Config;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IProtoBatchBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.JStormDebugger;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.utils.JStormUtils;
import com.esotericsoftware.kryo.KryoException;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TaskReceiver {
    private static Logger LOG = LoggerFactory.getLogger(TaskReceiver.class);

    protected Map stormConf;
    protected Task task;
    protected final int taskId;
    protected final IBolt bolt;
    protected final String idStr;

    protected TopologyContext topologyContext;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;

    protected DisruptorQueue deserializeQueue;
    // protected KryoTupleDeserializer deserializer;
    protected List<AsyncLoopThread> deserializeThreads;
    protected AsmHistogram deserializeTimer;

    protected volatile TaskStatus taskStatus;

    protected int dserializeThreadNum;

    private boolean isBackpressureEnable;
    private float lowMark;
    private float highMark;
    private volatile boolean backpressureStatus;

    public TaskReceiver(Task task, int taskId, Map stormConf, TopologyContext topologyContext, Map<Integer, DisruptorQueue> innerTaskTransfer,
                        TaskStatus taskStatus, String taskName) {
        this.stormConf = stormConf;
        this.task = task;
        this.bolt = (task.getTaskObj() instanceof IBolt ? (IBolt) task.getTaskObj() : null);
        this.taskId = taskId;
        this.idStr = taskName;

        this.topologyContext = topologyContext;
        this.innerTaskTransfer = innerTaskTransfer;

        this.taskStatus = taskStatus;

        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);

        long timeout = JStormUtils.parseLong(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT), 10);
        WaitStrategy waitStrategy = new TimeoutBlockingWaitStrategy(timeout, TimeUnit.MILLISECONDS);
        boolean isDisruptorBatchMode = ConfigExtension.isDisruptorQueueBatchMode(stormConf);
        int disruptorBatch = ConfigExtension.getDisruptorBufferSize(stormConf);
        long flushMs = ConfigExtension.getDisruptorBufferFlushMs(stormConf);
        this.deserializeQueue = DisruptorQueue.mkInstance("TaskDeserialize", ProducerType.MULTI, queueSize, waitStrategy, 
        		isDisruptorBatchMode, disruptorBatch, flushMs);
        dserializeThreadNum = ConfigExtension.getTaskDeserializeThreadNum(stormConf);
        deserializeThreads = new ArrayList<AsyncLoopThread>();
        setDeserializeThread();
        //this.deserializer = new KryoTupleDeserializer(stormConf, topologyContext);

        String topologyId = topologyContext.getTopologyId();
        String component = topologyContext.getThisComponentId();

        AsmHistogram deserializeTimerHistogram = new AsmHistogram();
        deserializeTimerHistogram.setAggregate(false);
        deserializeTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topologyId, component, taskId, MetricDef.DESERIALIZE_TIME, MetricType.HISTOGRAM), deserializeTimerHistogram);

        QueueGauge deserializeQueueGauge = new QueueGauge(deserializeQueue, idStr, MetricDef.DESERIALIZE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                        topologyId, component, taskId, MetricDef.DESERIALIZE_QUEUE, MetricType.GAUGE),
                new AsmGauge(deserializeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.DESERIALIZE_QUEUE, deserializeQueueGauge);

        this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        this.highMark = (float) ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        this.lowMark = (float) ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        this.backpressureStatus = false;

        LOG.info("Successfully start TaskReceiver thread for {}, thread num: {}", idStr, dserializeThreadNum);
    }

    public List<AsyncLoopThread> getDeserializeThread() {
        return deserializeThreads;
    }

    protected void setDeserializeThread() {
        for (int i = 0; i < dserializeThreadNum; i++) {
            deserializeThreads.add(new AsyncLoopThread(new DeserializeRunnable(deserializeQueue, innerTaskTransfer.get(taskId), i)));
        }
    }

    public DisruptorQueue getDeserializeQueue() {
        return deserializeQueue;
    }


    class DeserializeRunnable extends RunnableCallback implements EventHandler {

        DisruptorQueue deserializeQueue;
        DisruptorQueue exeQueue;
        int threadIndex;
        KryoTupleDeserializer deserializer;

        DeserializeRunnable(DisruptorQueue deserializeQueue, DisruptorQueue exeQueue, int threadIndex) {
            this.deserializeQueue = deserializeQueue;
            this.exeQueue = exeQueue;
            this.threadIndex = threadIndex;
            this.deserializer = new KryoTupleDeserializer(stormConf, topologyContext, topologyContext.getRawTopology());
        }

        @Override
        public String getThreadName() {
            return idStr + "-deserializer-" + threadIndex;
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
            deserialize(deserializer, (byte[]) event, exeQueue);
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
            //deserializeQueue.consumerStarted();
            LOG.info("Successfully start recvThread of {}, {}", idStr, threadIndex);
            while (!taskStatus.isShutdown()) {
                try {
                    deserializeQueue.multiConsumeBatchWhenAvailableWithCallback(this);
                } catch (Throwable e) {
                    if (!taskStatus.isShutdown()) {
                        LOG.error("Unknow exception ", e);
                    }
                }
            }
            task.unregisterDeserializeQueue();
            LOG.info("Successfully shutdown recvThread of " + idStr);
        }

        public Object getResult() {
            LOG.info("Begin to shutdown recvThread of " + idStr);
            return -1;
        }
    }

    public boolean deserializer(KryoTupleDeserializer deserializer, boolean forceConsume) {
        //LOG.debug("start Deserializer of task, {}", taskId);
        boolean isIdling = true;
        DisruptorQueue exeQueue = innerTaskTransfer.get(taskId);
        if (!taskStatus.isShutdown()) {
            if ((deserializeQueue.population() > 0 && exeQueue.pctFull() < 1.0) || forceConsume) {
                try {
                    List<Object> objects = deserializeQueue.retreiveAvailableBatch();
                    for (Object object : objects) {
                        deserialize(deserializer, (byte[]) object, exeQueue);
                    }
                    isIdling = false;
                } catch (InterruptedException e) {
                    LOG.error("InterruptedException " + e.getCause());
                    return isIdling;
                } catch (TimeoutException e) {
                    return isIdling;
                } catch (Throwable e) {
                    if (Utils.exceptionCauseIsInstanceOf(KryoException.class, e)) {
                        throw new RuntimeException(e);
                    } else if (!taskStatus.isShutdown()) {
                        LOG.error("Unknow exception ", e);
                    }
                }
            }
        } else {
            task.unregisterDeserializeQueue();
        }
        return isIdling;
    }

    protected void deserialize(KryoTupleDeserializer deserializer, byte[] serMsg, DisruptorQueue queue) {
        long start = deserializeTimer.getTime();
        try {
            if (serMsg == null || serMsg.length == 0) {
                return;
            }

            if (serMsg.length == 1) {
                byte newStatus = serMsg[0];
                LOG.info("Change task status as " + newStatus);
                taskStatus.setStatus(newStatus);
                return;
            }

            // ser_msg.length > 1
            if (bolt != null && bolt instanceof IProtoBatchBolt) {
                List<byte[]> serMsgs = ((IProtoBatchBolt) bolt).protoExecute(serMsg);
                if (serMsgs != null) {
                    for (byte[] data : serMsgs) {
                        deserializeTuple(deserializer, data, queue);
                    }
                } else {
                    deserializeTuple(deserializer, serMsg, queue);
                }
            } else {
                deserializeTuple(deserializer, serMsg, queue);
            }
            
        }  catch (Throwable e) {
            if (Utils.exceptionCauseIsInstanceOf(KryoException.class, e))
                throw new RuntimeException(e);
            if (!taskStatus.isShutdown()) {
                LOG.error(idStr + " recv thread error " + JStormUtils.toPrintableString(serMsg) + "\n", e);
            }
        } finally {
            if (MetricUtils.metricAccurateCal)
                deserializeTimer.updateTime(start);
        }
    }

    protected void deserializeTuple(KryoTupleDeserializer deserializer, byte[] serMsg, DisruptorQueue queue) {
        Tuple tuple = deserializer.deserialize(serMsg);
        if (tuple != null) {
            if (JStormDebugger.isDebugRecv(tuple.getMessageId())) {
                LOG.info(idStr + " receive " + tuple.toString());
            }
            //queue.publish(tuple);
            if (isBackpressureEnable) {
                if (backpressureStatus) {
                    while (queue.pctFull() > lowMark) {
                        JStormUtils.sleepMs(1);
                    }
                    queue.publish(tuple);
                    backpressureStatus = false;
                } else  {
                    queue.publish(tuple);
                    if (queue.pctFull() > highMark) {
                        backpressureStatus = true;
                    }
                }
            } else {
                queue.publish(tuple);
            }
        }
    }
}