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

import backtype.storm.spout.SpoutOutputCollectorCb;
import backtype.storm.task.ICollectorCallback;

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
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeOutMap;

import backtype.storm.Config;
import backtype.storm.spout.ISpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;


/**
 * spout collector, sending tuple through this Object
 *
 * @author yannian/Longda
 */
public class SpoutCollector extends SpoutOutputCollectorCb {
    private static Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);

    protected TaskSendTargets sendTargets;
    protected Map storm_conf;
    protected TaskTransfer transfer_fn;
    // protected TimeCacheMap pending;
    protected TimeOutMap<Long, TupleInfo> pending;
    // topology_context is system topology context
    protected TopologyContext topology_context;

    protected DisruptorQueue disruptorAckerQueue;
    protected TaskBaseMetric task_stats;
    protected backtype.storm.spout.ISpout spout;
    protected ITaskReportErr report_error;

    protected Integer task_id;
    protected Integer ackerNum;

    protected AsmHistogram emitTotalTimer;
    protected Random random;

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
        this.spout = (ISpout) task.getTaskObj();
        this.task_id = task.getTaskId();
        this.report_error = task.getReportErrorDie();

        ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));

        random = new Random(Utils.secureRandomLong());
/*        random.setSeed(System.currentTimeMillis());*/

        String componentId = topology_context.getThisComponentId();
        emitTotalTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topology_context.getTopologyId(), componentId, task_id, MetricDef.COLLECTOR_EMIT_TIME,
                MetricType.HISTOGRAM), new AsmHistogram());
        emitTotalTimer.setEnabled(false);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return sendSpoutMsg(streamId, tuple, messageId, null, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId, null);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        return sendSpoutMsg(streamId, tuple, messageId, null, callback);
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback) {
        sendSpoutMsg(streamId, tuple, messageId, taskId, callback);
    }

    public void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendCtrlMsg(streamId, tuple, messageId, taskId);
    }

    public List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId) {
        return sendCtrlMsg(streamId, tuple, messageId, null);
    }

    protected List<Integer> sendSpoutMsg(String outStreamId, List<Object> values, Object messageId, Integer outTaskId, ICollectorCallback callback) {
        java.util.List<Integer> outTasks = null;
        outTasks = sendMsg(outStreamId, values, messageId, outTaskId, callback);
        return outTasks;
    }

    void unanchoredSend(TopologyContext topologyContext, TaskSendTargets taskTargets, TaskTransfer transfer_fn, String stream, List<Object> values){
        UnanchoredSend.send(topologyContext, taskTargets, transfer_fn, stream, values);
    }

    void transferCtr(TupleImplExt tupleExt){
        transfer_fn.transferControl(tupleExt);
    }

    protected void sendMsgToAck(String out_stream_id, List<Object> values, Object message_id, Long root_id, List<Long> ackSeq, boolean needAck){
        if (needAck) {
            TupleInfo info = new TupleInfo();
            info.setStream(out_stream_id);
            info.setValues(values);
            info.setMessageId(message_id);
            info.setTimestamp(System.currentTimeMillis());

            pending.putHead(root_id, info);

            List<Object> ackerTuple = JStormUtils.mk_list((Object) root_id, JStormUtils.bit_xor_vals(ackSeq), task_id);

            unanchoredSend(topology_context, sendTargets, transfer_fn, Acker.ACKER_INIT_STREAM_ID, ackerTuple);

        } else if (message_id != null) {
            TupleInfo info = new TupleInfo();
            info.setStream(out_stream_id);
            info.setValues(values);
            info.setMessageId(message_id);
            info.setTimestamp(0);

            AckSpoutMsg ack = new AckSpoutMsg(root_id, spout, null, info, task_stats);
            ack.run();
        }
    }

    public List<Integer> sendMsg(String out_stream_id, List<Object> values, Object message_id, Integer out_task_id,  ICollectorCallback callback) {
        final long startTime = emitTotalTimer.getTime();
        try {
            boolean needAck = (message_id != null) && (ackerNum > 0);
            Long root_id = getRootId(message_id);
            List<Integer> outTasks = null;

            if (out_task_id != null) {
                outTasks = sendTargets.get(out_task_id, out_stream_id, values, null, root_id);
            } else {
                outTasks = sendTargets.get(out_stream_id, values, null, root_id);
            }

            List<Long> ackSeq = new ArrayList<Long>();
            for (Integer t : outTasks) {
                MessageId msgid;
                if (needAck) {
                    // Long as = MessageId.generateId();
                    Long as = MessageId.generateId(random);
                    msgid = MessageId.makeRootId(root_id, as);
                    ackSeq.add(as);
                } else {
                    msgid = null;
                }

                TupleImplExt tp = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                transfer_fn.transfer(tp);
            }
            sendMsgToAck(out_stream_id, values,  message_id,  root_id, ackSeq, needAck);
            if (callback != null)
                callback.execute(out_stream_id, outTasks, values);
            return outTasks;
        } finally {
            emitTotalTimer.updateTime(startTime);
        }
    }

    protected List<Integer> sendCtrlMsg(String out_stream_id, List<Object> values, Object message_id, Integer out_task_id) {
        final long startTime = emitTotalTimer.getTime();
        try {
            boolean needAck = (message_id != null) && (ackerNum > 0);
            Long root_id = getRootId(message_id);
            java.util.List<Integer> out_tasks = null;

            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values, null, root_id);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values, null, root_id);
            }

            List<Long> ackSeq = new ArrayList<Long>();
            for (Integer t : out_tasks) {
                MessageId msgid;
                if (needAck) {
                    // Long as = MessageId.generateId();
                    Long as = MessageId.generateId(random);
                    msgid = MessageId.makeRootId(root_id, as);
                    ackSeq.add(as);
                } else {
                    msgid = null;
                }

                TupleImplExt tp = new TupleImplExt(topology_context, values, task_id, out_stream_id, msgid);
                tp.setTargetTaskId(t);
                transferCtr(tp);
            }
            sendMsgToAck(out_stream_id, values,  message_id,  root_id, ackSeq, needAck);
            return out_tasks;
        } finally {
            emitTotalTimer.updateTime(startTime);
        }
    }

    @Override
    public void reportError(Throwable error) {
        report_error.report(error);
    }

    protected Long getRootId(Object messageId) {
        Boolean needAck = (messageId != null) && (ackerNum > 0);

        // This change storm logic
        // Storm can't make sure root_id is unique
        // storm's logic is root_id = MessageId.generateId(random);
        // when duplicate root_id, it will miss call ack/fail
        Long rootId = null;
        if (needAck) {
            rootId = MessageId.generateId(random);
/*            while (pending.containsKey(rootId)) {
                rootId = MessageId.generateId(random);
            }*/
        }
        return rootId;
    }

}
