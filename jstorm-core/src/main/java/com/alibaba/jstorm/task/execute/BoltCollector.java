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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.OutputCollectorCb;

import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;

/**
 * bolt output interface, do emit/ack/fail
 *
 * @author yannian/Longda
 */
public class BoltCollector extends OutputCollectorCb {
    private static Logger LOG = LoggerFactory.getLogger(BoltCollector.class);

    protected ITaskReportErr reportError;
    protected TaskSendTargets sendTargets;
    protected TaskTransfer taskTransfer;
    protected TopologyContext topologyContext;
    protected Integer task_id;
    // protected TimeCacheMap<Tuple, Long> tuple_start_times;
    protected final RotatingMap<Tuple, Long> tuple_start_times;
    protected TaskBaseMetric task_stats;
    // protected TimeCacheMap<Tuple, Long> pending_acks;
    protected final RotatingMap<Tuple, Long> pending_acks;
    protected long lastRotate = System.currentTimeMillis();
    protected long rotateTime;

    protected Map storm_conf;
    protected Integer ackerNum;
    protected AsmHistogram emitTimer;
    protected Random random;


    //ITaskReportErr report_error, TaskSendTargets _send_fn, Map _storm_conf, TaskTransfer _transfer_fn,
    //TopologyContext _topology_context, Integer task_id,  TaskBaseMetric _task_stats
    public BoltCollector(Task task, RotatingMap<Tuple, Long> tuple_start_times, int message_timeout_secs) {

        this.rotateTime = 1000L * message_timeout_secs / (Acker.TIMEOUT_BUCKET_NUM - 1);
        this.reportError = task.getReportErrorDie();
        this.sendTargets = task.getTaskSendTargets();
        this.storm_conf = task.getStormConf();
        this.taskTransfer = task.getTaskTransfer();
        this.topologyContext = task.getTopologyContext();
        this.task_id = task.getTaskId();
        this.task_stats = task.getTaskStats();

        this.pending_acks = new RotatingMap<Tuple, Long>(Acker.TIMEOUT_BUCKET_NUM);
        // this.pending_acks = new TimeCacheMap<Tuple,
        // Long>(message_timeout_secs,
        // Acker.TIMEOUT_BUCKET_NUM);
        this.tuple_start_times = tuple_start_times;

        this.ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));

        String componentId = topologyContext.getThisComponentId();
        this.emitTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                        topologyContext.getTopologyId(), componentId, task_id, MetricDef.COLLECTOR_EMIT_TIME, MetricType.HISTOGRAM),
                new AsmHistogram());
        this.emitTimer.setEnabled(false);
        //this.emitTimer.setTimeUnit(TimeUnit.NANOSECONDS);

        random = new Random(Utils.secureRandomLong());
/*        random.setSeed(System.currentTimeMillis());*/
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return sendBoltMsg(streamId, anchors, tuple, null, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        sendBoltMsg(streamId, anchors, tuple, taskId, null);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        return sendBoltMsg(streamId, anchors, tuple, null, callback);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback) {
        sendBoltMsg(streamId, anchors, tuple, taskId, callback);
    }

    public List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return sendCtrlMsg(streamId, tuple, anchors, null);
    }

    public void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        sendCtrlMsg(streamId, tuple, anchors, taskId);
    }

    protected List<Integer> sendBoltMsg(String outStreamId, Collection<Tuple> anchors, List<Object> values, Integer outTaskId,
                                        ICollectorCallback callback) {
        java.util.List<Integer> outTasks = null;
        outTasks = sendMsg(outStreamId, values, anchors, outTaskId, callback);
        return outTasks;
    }

    protected MessageId getMessageId(Collection<Tuple> anchors) {
        MessageId ret = null;
        if (anchors != null && ackerNum > 0) {
            Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
            for (Tuple a : anchors) {
            	if (a.getMessageId() != null) {
                    Long edge_id = MessageId.generateId(random);
                    put_xor(pending_acks, a, edge_id);
                    MessageId messageId = a.getMessageId();
                    if (messageId != null){
                        for (Long root_id : messageId.getAnchorsToIds().keySet()) {
                            put_xor(anchors_to_ids, root_id, edge_id);
                        }
                    }
            	}
            }
            ret = MessageId.makeId(anchors_to_ids);
        }
        return ret;
    }

    public List<Integer> sendMsg(String out_stream_id, List<Object> values, Collection<Tuple> anchors, Integer out_task_id, ICollectorCallback callback) {
        final long start = emitTimer.getTime();
        List<Integer> outTasks = null;
        try {

            if (out_task_id != null) {
                outTasks = sendTargets.get(out_task_id, out_stream_id, values, anchors, null);
            } else {
                outTasks = sendTargets.get(out_stream_id, values, anchors, null);
            }

            tryRotate();
            for (Integer t : outTasks) {
                MessageId msgid = getMessageId(anchors);

                TupleImplExt tp = new TupleImplExt(topologyContext, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                taskTransfer.transfer(tp);
            }
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            if (outTasks == null) {
                outTasks = new ArrayList<Integer>();
            }
            if (callback != null)
                callback.execute(out_stream_id, outTasks,  values);
            emitTimer.updateTime(start);
        }
        return outTasks;
    }

    private void tryRotate() {
        long now = System.currentTimeMillis();
        if (now - lastRotate > rotateTime) {
            pending_acks.rotate();
            lastRotate = now;
        }
    }

    void unanchoredSend(TopologyContext topologyContext, TaskSendTargets taskTargets, TaskTransfer transfer_fn, String stream, List<Object> values){
        UnanchoredSend.send(topologyContext, taskTargets, transfer_fn, stream, values);
    }

    void transferCtr(TupleImplExt tupleExt){
        taskTransfer.transferControl(tupleExt);
    }

    protected List<Integer> sendCtrlMsg(String out_stream_id, List<Object> values, Collection<Tuple> anchors, Integer out_task_id) {
        final long start = emitTimer.getTime();
        java.util.List<Integer> out_tasks = null;
        try {

            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values, anchors, null);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values, anchors, null);
            }

            tryRotate();
            for (Integer t : out_tasks) {
                MessageId msgid = getMessageId(anchors);

                TupleImplExt tp = new TupleImplExt(topologyContext, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                transferCtr(tp);
            }
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            emitTimer.updateTime(start);
        }
        return out_tasks;
    }

    @Override
    public void ack(Tuple input) {
        if (input.getMessageId() != null) {
            Long ack_val = 0L;
            Object pend_val = pending_acks.remove(input);
            if (pend_val != null) {
                ack_val = (Long) (pend_val);
            }

            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                unanchoredSend(topologyContext, sendTargets, taskTransfer, Acker.ACKER_ACK_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey(), JStormUtils.bit_xor(e.getValue(), ack_val)));
            }
        }

        Long latencyStart = (Long) tuple_start_times.remove(input);
        task_stats.bolt_acked_tuple(input.getSourceComponent(), input.getSourceStreamId());
        if (latencyStart != null && JStormMetrics.enabled) {
            long endTime = System.currentTimeMillis();
            task_stats.update_bolt_acked_latency(input.getSourceComponent(), input.getSourceStreamId(),
                    latencyStart, endTime);
        }
    }

    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (input.getMessageId() != null) {
            pending_acks.remove(input);
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                unanchoredSend(topologyContext, sendTargets, taskTransfer, Acker.ACKER_FAIL_STREAM_ID, JStormUtils.mk_list((Object) e.getKey()));
            }
        }

        tuple_start_times.remove(input);
        task_stats.bolt_failed_tuple(input.getSourceComponent(), input.getSourceStreamId());
    }

    @Override
    public void reportError(Throwable error) {
        reportError.report(error);
    }

    public static void put_xor(RotatingMap<Tuple, Long> pending, Tuple key, Long id) {
        // synchronized (pending) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = 0L;
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
        // }
    }

    public static void put_xor(Map<Long, Long> pending, Long key, Long id) {
        // synchronized (pending) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = 0L;
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
        // }
    }

}
