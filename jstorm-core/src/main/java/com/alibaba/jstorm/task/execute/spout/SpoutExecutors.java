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
package com.alibaba.jstorm.task.execute.spout;

import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.spout.CtrlMsgSpout;
import com.alibaba.jstorm.client.spout.ICtrlMsgSpout;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.TimerRatio;
import com.alibaba.jstorm.daemon.worker.JStormDebugger;
import com.alibaba.jstorm.daemon.worker.timer.TimerConstants;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.RotatingMap;
import com.alibaba.jstorm.utils.TimeUtils;
import com.codahale.metrics.Gauge;
import com.lmax.disruptor.EventHandler;

import backtype.storm.Config;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;
import com.alibaba.jstorm.task.execute.spout.FailSpoutMsg;
import com.alibaba.jstorm.task.execute.spout.AckSpoutMsg;

/**
 * spout executor
 * <p/>
 * All spout actions will be done here
 *
 * @author yannian/Longda
 */
public class SpoutExecutors extends BaseExecutors implements EventHandler {
    private static Logger LOG = LoggerFactory.getLogger(SpoutExecutors.class);

    protected final Integer max_spout_pending;

    protected backtype.storm.spout.ISpout spout;
    protected RotatingMap<Long, TupleInfo> pending;

    protected SpoutOutputCollector outputCollector;

    protected AsmHistogram nextTupleTimer;
    protected TimerRatio emptyCpuGauge;

    private String topologyId;
    private String componentId;
    private int taskId;

    protected AsyncLoopThread ackerRunnableThread;

    protected boolean isSpoutFullSleep;

    protected volatile boolean checkTopologyFinishInit = false;

    //, backtype.storm.spout.ISpout _spout, TaskTransfer _transfer_fn, Map<Integer, DisruptorQueue> innerTaskTransfer,
    //Map _storm_conf, TaskSendTargets sendTargets, TaskStatus taskStatus, TopologyContext topology_context, TopologyContext _user_context,
    //TaskBaseMetric _task_stats, ITaskReportErr _report_error, JStormMetricsReporter metricReporter
    public SpoutExecutors(Task task) {
        super(task);

        this.spout = (ISpout) task.getTaskObj();

        int ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        if (ackerNum != 0){
            this.max_spout_pending = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
        }else {
            this.max_spout_pending = null;
        }
        this.topologyId = sysTopologyCtx.getTopologyId();
        this.componentId = sysTopologyCtx.getThisComponentId();
        this.taskId = task.getTaskId();

        this.nextTupleTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topologyId, componentId, taskId, MetricDef.EXECUTE_TIME, MetricType.HISTOGRAM), new AsmHistogram());

        this.emptyCpuGauge = new TimerRatio();
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, componentId, taskId, MetricDef.EMPTY_CPU_RATIO, MetricType.GAUGE),
                new AsmGauge(emptyCpuGauge));

        isSpoutFullSleep = ConfigExtension.isSpoutPendFullSleep(storm_conf);

        LOG.info("isSpoutFullSleep:" + isSpoutFullSleep);

        mkPending();

        JStormMetrics.registerTaskMetric(
                MetricUtils.taskMetricName(topologyId, componentId, taskId, MetricDef.PENDING_MAP, MetricType.GAUGE), new AsmGauge(
                        new Gauge<Double>() {
                            @Override
                            public Double getValue() {
                                return (double) pending.size();
                            }
                        }));

        // collector, in fact it call send_spout_msg
        SpoutCollector collector = null;
        if (ConfigExtension.isTaskBatchTuple(storm_conf)) {
            collector = new SpoutBatchCollector(task, pending, exeQueue);
        } else {
            collector = new SpoutCollector(task, pending, exeQueue);
        }

        this.outputCollector = new SpoutOutputCollector(collector);
        //this task don't continue until it bulid connection with topologyMaster
        Integer topologyMasterId = sysTopologyCtx.getTopologyMasterId();
        List<Integer> localWorkerTasks = sysTopologyCtx.getThisWorkerTasks();
        if (topologyMasterId != 0 && !localWorkerTasks.contains(topologyMasterId)){
            while (getConnection(topologyMasterId) == null){
                JStormUtils.sleepMs(10);
                LOG.info("this task still is building connection with topology Master");
            }
        }

        taskHbTrigger.setSpoutOutputCollector(collector);
        taskHbTrigger.register();

        LOG.info("Successfully create SpoutExecutors " + idStr);
    }

    public void mkPending() {
        // this function will be override by subclass
        throw new RuntimeException("Should override this function");
    }

    @Override
    public void init() throws Exception {

        this.spout.open(storm_conf, userTopologyCtx, outputCollector);
        //send the HbMsg to TM, but it don't really finish spout's init.
        taskHbTrigger.sendHbMsg();
        LOG.info("Successfully open SpoutExecutors " + idStr);
    }

    public void nextTuple() {
        if (!taskStatus.isRun()) {
            JStormUtils.sleepMs(1);
            return;
        }

        // if don't need ack, pending map will be always empty
        if (max_spout_pending == null || pending.size() < max_spout_pending) {
            emptyCpuGauge.stop();

            long start = nextTupleTimer.getTime();
            try {
                spout.nextTuple();
            } catch (Throwable e) {
                error = e;
                LOG.error("spout execute error ", e);
                report_error.report(e);
            } finally {
                nextTupleTimer.updateTime(start);
            }
        } else {
            if (isSpoutFullSleep) {
                JStormUtils.sleepMs(1);
            }
            emptyCpuGauge.start();
            // just return, no sleep
        }
    }

    @Override
    public void run() {
        throw new RuntimeException("Should implement this function");
    }

    /**
     * Handle acker message
     *
     * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long, boolean)
     */
    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        try {
            if (event == null) {
                return;
            }
            Runnable runnable = null;
            if (event instanceof Tuple) {
                if (((TupleExt) event).isBatchTuple()) {
            		List<Object> values = ((Tuple) event).getValues();
            		for (Object value : values) {
            			Pair<MessageId, List<Object>> val = (Pair<MessageId, List<Object>>) value;
            			TupleImplExt tuple = new TupleImplExt(sysTopologyCtx, val.getSecond(), val.getFirst(), ((TupleImplExt) event));
            			processControlEvent();
            			runnable = processTupleEvent(tuple);
            			if (runnable != null) {
                            runnable.run();
                            runnable = null;
                        }
            		}
            	} else {
                    runnable = processTupleEvent((Tuple) event);
            	}
            }else if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
                return;
            } else if (event instanceof IAckMsg) {
                runnable = (Runnable) event;
            } else if (event instanceof Runnable) {
                runnable = (Runnable) event;
            } else {

                LOG.warn("Receive one unknow event-" + event.toString() + " " + idStr);
                return;
            }

            if (runnable != null)
                runnable.run();

        } catch (Throwable e) {
            if (!taskStatus.isShutdown()) {
                LOG.info("Unknow excpetion ", e);
                report_error.report(e);
            }
        }
    }

    private Runnable processTupleEvent(Tuple event) {
        Runnable runnable = null;
        Tuple tuple = (Tuple) event;
        if (event.getSourceStreamId().equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
            TopoMasterCtrlEvent ctrlEvent = (TopoMasterCtrlEvent) tuple.getValueByField("ctrlEvent");
            if (ctrlEvent.isTransactionEvent()) {
               	if (spout instanceof ICtrlMsgSpout) {
               	    runnable = new CtrlMsgSpout((ICtrlMsgSpout) spout, ctrlEvent);
               	}
            } else if (ctrlEvent.isFinishInitEvent()){
                LOG.info("spout task-{} received topology finish init operation message", taskId);
                taskHbTrigger.updateExecutorStatus(TaskStatus.RUN);
                this.checkTopologyFinishInit = true;
            } else {
                LOG.warn("Received unexpected control event, {}", ctrlEvent);
            }
        } else if (event.getSourceStreamId().equals(Common.TOPOLOGY_MASTER_REGISTER_METRICS_RESP_STREAM_ID)) {
            this.metricsReporter.updateMetricMeta((Map<String, Long>) tuple.getValue(0));
        } else {
            Object id = tuple.getValue(0);
            Object obj = pending.remove((Long) id);

            if (obj == null) {
                if (JStormDebugger.isDebug(id)) {
                    LOG.info("Pending map no entry:" + id);
                }
                runnable = null;
            } else {
                TupleInfo tupleInfo = (TupleInfo) obj;

                String stream_id = tuple.getSourceStreamId();

                if (stream_id.equals(Acker.ACKER_ACK_STREAM_ID)) {
                    runnable = new AckSpoutMsg(id, spout, tuple, tupleInfo, task_stats);
                } else if (stream_id.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    runnable = new FailSpoutMsg(id, spout, tupleInfo, task_stats);
                } else {
                    LOG.warn("Receive one unknown source Tuple " + idStr);
                    runnable = null;
                }
            }

            task_stats.recv_tuple(tuple.getSourceComponent(), tuple.getSourceStreamId());
        }
        return runnable;
    }

    public AsyncLoopThread getAckerRunnableThread() {
        return ackerRunnableThread;
    }

    private void processTimerEvent(TimerTrigger.TimerEvent event) {
        switch (event.getOpCode()) {
            case TimerConstants.ROTATING_MAP: {
                Map<Long, TupleInfo> timeoutMap = pending.rotate();
                for (java.util.Map.Entry<Long, TupleInfo> entry : timeoutMap.entrySet()) {
                    TupleInfo tupleInfo = entry.getValue();
                    FailSpoutMsg fail = new FailSpoutMsg(entry.getKey(), spout, (TupleInfo) tupleInfo, task_stats);
                    fail.run();
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
        	Runnable runnable = null;
            if (event instanceof TimerTrigger.TimerEvent) {
                processTimerEvent((TimerTrigger.TimerEvent) event);
            } else if (event instanceof Tuple) {
                runnable = processTupleEvent((Tuple) event);
            } else {
                LOG.warn("Received unknown control event, " + event.getClass().getName());
            }

            if (runnable != null) {
            	runnable.run();
            }
        }
    }
    
    public Object getOutputCollector() {
        return outputCollector;
    }
}
