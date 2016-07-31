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
package com.alibaba.jstorm.task.execute;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.daemon.worker.timer.BackpressureCheckTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TickTupleTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TimerConstants;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.task.*;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.backpressure.BackpressureTrigger;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.alibaba.jstorm.utils.TimeUtils;
import com.lmax.disruptor.EventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * BoltExecutor
 *
 * @author yannian/Longda
 */
public class BoltExecutors extends BaseExecutors implements EventHandler {
    private static Logger LOG = LoggerFactory.getLogger(BoltExecutors.class);

    protected IBolt bolt;

    protected RotatingMap<Tuple, Long> tuple_start_times;

    private int ackerNum = 0;

    // internal outputCollector is BoltCollector
    private OutputCollector outputCollector;

    /**
     * execute time, used for backpressure, in ms.
     */
    private volatile double exeTime;

    private BackpressureTrigger backpressureTrigger;
    private boolean isSystemBolt;

    //, IBolt _bolt, TaskTransfer _transfer_fn, Map<Integer, DisruptorQueue> innerTaskTransfer, Map storm_conf,
    //TaskSendTargets _send_fn, TaskStatus taskStatus, TopologyContext sysTopologyCxt, TopologyContext userTopologyCxt, TaskBaseMetric _task_stats,
    //ITaskReportErr _report_error, JStormMetricsReporter metricReport
    public BoltExecutors(Task task) {
        super(task);

        this.bolt = (IBolt) task.getTaskObj();

        // create TimeCacheMap
        this.tuple_start_times = new RotatingMap<Tuple, Long>(Acker.TIMEOUT_BUCKET_NUM);
        this.ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));

        // create BoltCollector
        BoltCollector output_collector = null;
        if (ConfigExtension.isTaskBatchTuple(storm_conf)) {
            output_collector = new BoltBatchCollector(task, tuple_start_times, message_timeout_secs);
        } else {
            output_collector = new BoltCollector(task, tuple_start_times, message_timeout_secs);
        }
        outputCollector = new OutputCollector(output_collector);
        taskHbTrigger.setBoltOutputCollector(output_collector);

        Object tickFrequence = storm_conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        isSystemBolt = Common.isSystemComponent(componentId);
        if (tickFrequence != null && !isSystemBolt) {
            Integer frequence = JStormUtils.parseInt(tickFrequence);
            TickTupleTrigger tickTupleTrigger = new TickTupleTrigger(sysTopologyCtx, frequence, idStr + Constants.SYSTEM_TICK_STREAM_ID, controlQueue);
            tickTupleTrigger.register();
        }

        if (!isSystemBolt) {
            backpressureTrigger = new BackpressureTrigger(task, this, storm_conf, output_collector);
            int backpressureCheckFrequence = ConfigExtension.getBackpressureCheckIntervl(storm_conf);
            BackpressureCheckTrigger backpressureCheckTrigger =
                    new BackpressureCheckTrigger(30, backpressureCheckFrequence, idStr + " backpressure check trigger", backpressureTrigger);
            backpressureCheckTrigger.register(TimeUnit.MILLISECONDS);
        }

        LOG.info("Successfully create BoltExecutors " + idStr);
    }

    @Override
    public void init() {
        bolt.prepare(storm_conf, userTopologyCtx, outputCollector);
        LOG.info("Succeesfully do Bolt.prepare");
        taskHbTrigger.register();
    }

    @Override
    public String getThreadName() {
        return idStr + "-" + BoltExecutors.class.getSimpleName();
    }

    @Override
    public void run() {
        if (!isFinishInit) {
            initWrapper();
        }
        while (!taskStatus.isShutdown()) {
            try {
                //if (backpressureTrigger != null)
                //    backpressureTrigger.checkAndTrigger();
                controlQueue.consumeBatch(this);
                exeQueue.consumeBatchWhenAvailable(this);
/*                processControlEvent();*/
            } catch (Throwable e) {
                if (!taskStatus.isShutdown()) {
                    LOG.error(idStr + " bolt exeutor  error", e);
                }
            }
        }
    }

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        if (event == null) {
            return;
        }

        long start = System.currentTimeMillis();
        try {
            if (event instanceof Tuple) {
/*                processControlEvent();*/
                processTupleEvent((Tuple) event);
            } else if (event instanceof BatchTuple) {
                for (Tuple tuple : ((BatchTuple) event).getTuples()) {
/*                    processControlEvent();*/
                    processTupleEvent((Tuple) tuple);
                }
            } else if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
            } else {
                LOG.warn("Bolt executor received unknown message");
            }
        } finally {
            if (JStormMetrics.enabled) {
                exeTime = System.currentTimeMillis() - start;
            }
        }
    }

    private void processTupleEvent(Tuple tuple) {
        task_stats.recv_tuple(tuple.getSourceComponent(), tuple.getSourceStreamId());

        if(ackerNum > 0 && tuple.getMessageId().isAnchored()) {
            tuple_start_times.put(tuple, System.currentTimeMillis());
        }

        try {
            if (!isSystemBolt && tuple.getSourceStreamId().equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
                backpressureTrigger.handle(tuple);
            } else {
                bolt.execute(tuple);
            }
        } catch (Throwable e) {
            error = e;
            LOG.error("bolt execute error ", e);
            report_error.report(e);
        }

        if (ackerNum == 0) {
            // only when acker is disabled
            // get tuple process latency
            Long latencyStart = (Long) tuple_start_times.remove(tuple);
            if (latencyStart != null && JStormMetrics.enabled) {
                long endTime = System.currentTimeMillis();
                long lifeCycleStart = ((TupleExt) tuple).getCreationTimeStamp();
                task_stats.bolt_acked_tuple(
                        tuple.getSourceComponent(), tuple.getSourceStreamId(), latencyStart, lifeCycleStart, endTime);
            }
        }
    }

    private void processTimerEvent(TimerTrigger.TimerEvent event) {
        switch (event.getOpCode()) {
            case TimerConstants.ROTATING_MAP: {
                Map<Tuple, Long> timeoutMap = tuple_start_times.rotate();

                if (ackerNum > 0) {
                    // only when acker is enable
                    for (Entry<Tuple, Long> entry : timeoutMap.entrySet()) {
                        Tuple input = entry.getKey();
                        task_stats.bolt_failed_tuple(input.getSourceComponent(), input.getSourceStreamId());
                    }
                }
                break;
            }
            case TimerConstants.TICK_TUPLE: {
                try {
                    Tuple tuple = (Tuple) event.getMsg();
                    bolt.execute(tuple);
                } catch (Throwable e) {
                    error = e;
                    LOG.error("bolt execute error ", e);
                    report_error.report(e);
                }
                break;
            }
            case TimerConstants.TASK_HEARTBEAT: {
                taskHbTrigger.setExeThreadHbTime(TimeUtils.current_time_secs());
                break;
            }
            default: {
                LOG.warn("Receive unsupported timer event, opcode=" + event.getOpCode());
                break;
            }
        }
    }

    protected void processControlEvent() {
        Object event = controlQueue.poll();

        if (event != null) {
            if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
                LOG.debug("Received one event from control queue");
            } else if (event instanceof Tuple) {
                processTupleEvent((Tuple) event);
            } else if (event instanceof BatchTuple) {
                for (Tuple tuple : ((BatchTuple) event).getTuples()) {
                    processTupleEvent(tuple);
                }
            } else {
                LOG.warn("Received unknown control event, " + event.getClass().getName());
            }
        }
    }

    public double getExecuteTime() {
        return exeTime;
    }

    @Override
    public Object getOutputCollector() {
        return outputCollector;
    }
}
