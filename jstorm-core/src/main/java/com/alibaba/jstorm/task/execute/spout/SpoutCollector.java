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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeOutMap;
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.Config;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.DisruptorQueue;

/**
 * spout collector, sending tuple through this Object
 * 
 * @author yannian/Longda
 */
public class SpoutCollector implements ISpoutOutputCollector {
    private static Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);

    private TaskSendTargets sendTargets;
    private Map storm_conf;
    private TaskTransfer transfer_fn;
    // private TimeCacheMap pending;
    private TimeOutMap<Long, TupleInfo> pending;
    // topology_context is system topology context
    private TopologyContext topology_context;

    private DisruptorQueue disruptorAckerQueue;
    private TaskBaseMetric task_stats;
    private backtype.storm.spout.ISpout spout;
    private ITaskReportErr report_error;

    private Integer task_id;
    private Integer ackerNum;
    private boolean isDebug = false;

    private AsmHistogram emitTotalTimer;
    Random random;

    //Integer task_id, backtype.storm.spout.ISpout spout, TaskBaseMetric task_stats, TaskSendTargets sendTargets, Map _storm_conf,
    //TaskTransfer _transfer_fn, TimeOutMap<Long, TupleInfo> pending, TopologyContext topology_context, DisruptorQueue disruptorAckerQueue,
    //ITaskReportErr _report_error
    public SpoutCollector(Task task, TimeOutMap<Long, TupleInfo> pending, DisruptorQueue disruptorAckerQueue) {
        this.sendTargets = task.getTaskSendTargets();
        this.storm_conf = task.getStormConf();
        this.transfer_fn = task.getTaskTransfer();
        this.pending = pending;
        this.topology_context = task.getTopologyContext();

        this.disruptorAckerQueue = disruptorAckerQueue;

        this.task_stats = task.getTaskStats();
        this.spout = (ISpout)task.getTaskObj();
        this.task_id = task.getTaskId();
        this.report_error = task.getReportErrorDie();

        ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        isDebug = JStormUtils.parseBoolean(storm_conf.get(Config.TOPOLOGY_DEBUG), false);

        random = new Random();
        random.setSeed(System.currentTimeMillis());

        String componentId = topology_context.getThisComponentId();
        emitTotalTimer =
                (AsmHistogram) JStormMetrics
                        .registerTaskMetric(MetricUtils.taskMetricName(topology_context.getTopologyId(), componentId, task_id, MetricDef.COLLECTOR_EMIT_TIME,
                                MetricType.HISTOGRAM), new AsmHistogram());
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return sendSpoutMsg(streamId, tuple, messageId, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId);
    }

    private List<Integer> sendSpoutMsg(String out_stream_id, List<Object> values, Object message_id, Integer out_task_id) {
        final long startTime = System.nanoTime();
        try {
            List<Integer> out_tasks;
            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values);
            }

            if (out_tasks.size() == 0) {
                // don't need send tuple to other task
                return out_tasks;
            }
            List<Long> ackSeq = new ArrayList<Long>();
            Boolean needAck = (message_id != null) && (ackerNum > 0);

            // This change storm logic
            // Storm can't make sure root_id is unique
            // storm's logic is root_id = MessageId.generateId(random);
            // when duplicate root_id, it will miss call ack/fail
            Long root_id = MessageId.generateId(random);
            if (needAck) {
                while (pending.containsKey(root_id)) {
                    root_id = MessageId.generateId(random);
                }
            }
            for (Integer t : out_tasks) {
                MessageId msgid;
                if (needAck) {
                    // Long as = MessageId.generateId();
                    Long as = MessageId.generateId(random);
                    msgid = MessageId.makeRootId(root_id, as);
                    ackSeq.add(as);
                } else {
                    msgid = MessageId.makeUnanchored();
                }

                TupleImplExt tp = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                transfer_fn.transfer(tp);
            }

            if (needAck) {
                TupleInfo info = new TupleInfo();
                info.setStream(out_stream_id);
                info.setValues(values);
                info.setMessageId(message_id);
                info.setTimestamp(System.nanoTime());

                pending.putHead(root_id, info);

                List<Object> ackerTuple = JStormUtils.mk_list((Object) root_id, JStormUtils.bit_xor_vals(ackSeq), task_id);

                UnanchoredSend.send(topology_context, sendTargets, transfer_fn, Acker.ACKER_INIT_STREAM_ID, ackerTuple);

            } else if (message_id != null) {
                TupleInfo info = new TupleInfo();
                info.setStream(out_stream_id);
                info.setValues(values);
                info.setMessageId(message_id);
                info.setTimestamp(0);

                AckSpoutMsg ack = new AckSpoutMsg(spout, null, info, task_stats, isDebug);
                ack.run();
            }

            return out_tasks;
        } finally {
            long endTime = System.nanoTime();
            emitTotalTimer.update((endTime - startTime) / TimeUtils.NS_PER_US);
        }

    }

    @Override
    public void reportError(Throwable error) {
        report_error.report(error);
    }

}
