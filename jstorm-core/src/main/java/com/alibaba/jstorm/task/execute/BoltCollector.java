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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMetric;
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
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.Config;
import backtype.storm.task.IOutputCollector;
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
public class BoltCollector implements IOutputCollector {
    private static Logger LOG = LoggerFactory.getLogger(BoltCollector.class);

    private ITaskReportErr reportError;
    private TaskSendTargets sendTargets;
    private TaskTransfer taskTransfer;
    private TopologyContext topologyContext;
    private Integer task_id;
    // private TimeCacheMap<Tuple, Long> tuple_start_times;
    private RotatingMap<Tuple, Long> tuple_start_times;
    private TaskBaseMetric task_stats;
    // private TimeCacheMap<Tuple, Long> pending_acks;
    private RotatingMap<Tuple, Long> pending_acks;
    private long lastRotate = System.currentTimeMillis();
    private long rotateTime;

    private Map storm_conf;
    private Integer ackerNum;
    private AsmMetric timer;
    private Random random;
    

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
        this.timer =
                JStormMetrics.registerTaskMetric(
                        MetricUtils.taskMetricName(topologyContext.getTopologyId(), componentId, task_id, MetricDef.COLLECTOR_EMIT_TIME, MetricType.HISTOGRAM),
                        new AsmHistogram());

        random = new Random();
        random.setSeed(System.currentTimeMillis());

    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return boltEmit(streamId, anchors, tuple, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        boltEmit(streamId, anchors, tuple, taskId);
    }

    private List<Integer> boltEmit(String out_stream_id, Collection<Tuple> anchors, List<Object> values, Integer out_task_id) {
        final long start = System.nanoTime();
        try {
            List<Integer> out_tasks;
            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values);
            }

            for (Integer t : out_tasks) {
                Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
                if (anchors != null) {
                    for (Tuple a : anchors) {
                        // Long edge_id = MessageId.generateId();
                        Long edge_id = MessageId.generateId(random);
                        long now = System.currentTimeMillis();
                        if (now - lastRotate > rotateTime) {
                            pending_acks.rotate();
                            lastRotate = now;
                        }
                        put_xor(pending_acks, a, edge_id);
                        for (Long root_id : a.getMessageId().getAnchorsToIds().keySet()) {
                            put_xor(anchors_to_ids, root_id, edge_id);
                        }
                    }
                }
                MessageId msgid = MessageId.makeId(anchors_to_ids);
                TupleImplExt tupleExt = new TupleImplExt(topologyContext, values, task_id, out_stream_id, msgid);
                tupleExt.setTargetTaskId(t);

                taskTransfer.transfer(tupleExt);
            }
            return out_tasks;
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            long end = System.nanoTime();
            timer.update((end - start) / TimeUtils.NS_PER_US);
        }
        return new ArrayList<Integer>();
    }

    @Override
    public void ack(Tuple input) {
        if (ackerNum > 0) {
            Long ack_val = 0L;
            Object pend_val = pending_acks.remove(input);
            if (pend_val != null) {
                ack_val = (Long) (pend_val);
            }

            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                UnanchoredSend.send(topologyContext, sendTargets, taskTransfer, Acker.ACKER_ACK_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey(), JStormUtils.bit_xor(e.getValue(), ack_val)));
            }
        }

        Long startTime = (Long) tuple_start_times.remove(input);
        if (startTime != null) {
        	Long endTime = System.nanoTime();
        	long latency = (endTime - startTime)/TimeUtils.NS_PER_US;
        	long lifeCycle = (System.currentTimeMillis() - ((TupleExt) input).getCreationTimeStamp()) * TimeUtils.NS_PER_US;
        	
            task_stats.bolt_acked_tuple(input.getSourceComponent(), input.getSourceStreamId(), latency, lifeCycle);
        }
    }

    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (ackerNum > 0) {
            pending_acks.remove(input);
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                UnanchoredSend.send(topologyContext, sendTargets, taskTransfer, Acker.ACKER_FAIL_STREAM_ID, JStormUtils.mk_list((Object) e.getKey()));
            }
        }

        task_stats.bolt_failed_tuple(input.getSourceComponent(), input.getSourceStreamId());
    }

    @Override
    public void reportError(Throwable error) {
        reportError.report(error);
    }

    // Utility functions, just used here
    public static Long tuple_time_delta(RotatingMap<Tuple, Long> start_times, Tuple tuple) {
        Long start_time = (Long) start_times.remove(tuple);
        if (start_time != null) {
            return (System.nanoTime() - start_time)/TimeUtils.NS_PER_US;
        }
        return null;
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
