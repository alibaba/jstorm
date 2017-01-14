package com.alibaba.jstorm.task.execute.spout;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.*;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.execute.BatchCollector;
import com.alibaba.jstorm.task.execute.MsgInfo;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.TimeOutMap;
import com.alibaba.jstorm.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaojian.fxj
 * @since 2.1.1
 */
public class SpoutBatchCollector extends SpoutCollector {
    private static Logger LOG = LoggerFactory.getLogger(SpoutBatchCollector.class);

    protected BatchCollector batchCollector;

    protected int batchSize;

    private CallIntervalGauge timeIntervalGauge;

    private final Map<Integer, Map<String, List<Object>>> pendingSendMsgs = new HashMap<Integer, Map<String, List<Object>>>();

    public SpoutBatchCollector(Task task, TimeOutMap<Long, TupleInfo> pending, DisruptorQueue disruptorAckerQueue) {
        super(task, pending, disruptorAckerQueue);

        String componentId = topology_context.getThisComponentId();

        timeIntervalGauge = new CallIntervalGauge();
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(task.getTopologyId(), componentId, task.getTaskId(), MetricDef.TASK_BATCH_INTERVAL_TIME, MetricType.GAUGE),
                new AsmGauge(timeIntervalGauge));

        batchCollector = new BatchCollector(task_id, componentId, storm_conf) {
            public void pushAndSend(String streamId, List<Object> tuple, Integer outTaskId, Collection<Tuple> anchors, Object messageId, Long rootId,
                                      ICollectorCallback callback) {
                if (outTaskId != null) {
                    synchronized (directBatches) {
                        List<MsgInfo> batchTobeFlushed = addToBatches(outTaskId.toString() + "-" + streamId, directBatches, streamId, tuple, outTaskId, messageId, rootId, batchSize,
                                callback);
                        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
                            timeIntervalGauge.incrementAndGet();
                            sendBatch(streamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed);
                        }
                    }
                } else {
                    synchronized (streamToBatches) {
                        List<MsgInfo> batchTobeFlushed = addToBatches(streamId, streamToBatches, streamId, tuple, outTaskId, messageId, rootId, batchSize,
                                callback);
                        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
                            timeIntervalGauge.incrementAndGet();
                            sendBatch(streamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed);
                        }
                    }
                }
            }

            public synchronized void flush() {
                synchronized (streamToBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : streamToBatches.entrySet()) {
                        List<MsgInfo> batch = streamToBatches.put(entry.getKey(), null);
                        if (batch != null && batch.size() > 0) {
                            sendBatch(entry.getKey(), null, batch);
                        }
                    }
                }
                synchronized (directBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : directBatches.entrySet()) {
                        List<MsgInfo> batch = directBatches.put(entry.getKey(), null);
                        if (batch != null && batch.size() > 0) {
                            // TaskId-StreamId --> [taskId, streamId]
                            String[] strings = entry.getKey().split("-", 2);
                            sendBatch(strings[1], strings[0], batch);
                        }
                    }
                }
            }
        };

        batchSize = batchCollector.getConfigBatchSize();
    }

    /**
     * @return if size of pending batch is bigger than the configured one, return the batch for sending, otherwise, return null
     */
    private List<Object> addToPendingSendBatch(int targetTask, String streamId, List<Object> values) {
        Map<String, List<Object>> streamToBatch = pendingSendMsgs.get(targetTask);
        if (streamToBatch == null) {
            streamToBatch = new HashMap<String, List<Object>>();
            pendingSendMsgs.put(targetTask, streamToBatch);

        }

        List<Object> batch = streamToBatch.get(streamId);
        if (batch == null) {
            batch = new ArrayList<Object>();
            streamToBatch.put(streamId, batch);
        }

        batch.addAll(values);
        if (batch.size() >= batchSize) {
            return batch;
        } else {
            return null;
        }
    }

    protected List<Integer> sendSpoutMsg(String outStreamId, List<Object> values, Object messageId, Integer outTaskId, ICollectorCallback callback) {
        /*java.util.List<Integer> outTasks = null;
        // LOG.info("spout push message to " + out_stream_id);
        List<MsgInfo> batchTobeFlushed = batchCollector.push(outStreamId, values, outTaskId, null, messageId, getRootId(messageId), callback);
        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
            outTasks = sendBatch(outStreamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed);
        }
        return outTasks;*/
        batchCollector.pushAndSend(outStreamId, values, outTaskId, null, messageId, getRootId(messageId), callback);
        return null;
    }

    public List<Integer> sendBatch(String outStreamId, String outTaskId, List<MsgInfo> batchTobeFlushed) {
        long startTime = emitTotalTimer.getTime();
        try {
            List<Integer> ret = null;
            Map<Object, List<MsgInfo>> outTasks;

            if (outTaskId != null) {
                outTasks = sendTargets.getBatch(Integer.valueOf(outTaskId), outStreamId, batchTobeFlushed);
            } else {
                outTasks = sendTargets.getBatch(outStreamId, batchTobeFlushed);
            }

            if (outTasks == null || outTasks.size() == 0) {
                // don't need send tuple to other task
                return new ArrayList<Integer>();
            }

            Map<Long, MsgInfo> ackBatch = new HashMap<Long, MsgInfo>();
            for (Map.Entry<Object, List<MsgInfo>> entry : outTasks.entrySet()) {
                Object target = entry.getKey();
                List<Integer> tasks = (target instanceof Integer) ? JStormUtils.mk_list((Integer) target) : ((List<Integer>) target);
                List<MsgInfo> batch = entry.getValue();

                for(int i = 0; i < tasks.size(); i++){
                    Integer t = tasks.get(i);
                    List<Object> batchValues = new ArrayList<Object>();
                    for (MsgInfo msg : batch) {
                        SpoutMsgInfo msgInfo = (SpoutMsgInfo) msg;
                        Pair<MessageId, List<Object>> pair = new Pair<MessageId, List<Object>>(getMessageId(msgInfo, ackBatch), msgInfo.values);
                        batchValues.add(pair);
                    }
                    TupleImplExt batchTuple = new TupleImplExt(topology_context, batchValues, task_id, outStreamId, null);
                    batchTuple.setTargetTaskId(t);
                    batchTuple.setBatchTuple(true);
                    transfer_fn.transfer(batchTuple);
                }

                for (MsgInfo msg : batch) {
                    if (msg.callback != null) {
                        msg.callback.execute(outStreamId, tasks, msg.values);
                    }
                }
            }


            if (ackBatch.size() > 0) {
                sendBatch(Acker.ACKER_INIT_STREAM_ID, null, new ArrayList<MsgInfo>(ackBatch.values()));
            }

            return ret;
        } finally {
            emitTotalTimer.updateTime(startTime);
        }

    }

    protected MessageId getMessageId(SpoutMsgInfo msg, Map<Long, MsgInfo> ackBatch) {
        MessageId msgId = null;
        if (msg.rootId != null) {
            Long as = MessageId.generateId(random);
            msgId = MessageId.makeRootId(msg.rootId, as);

            MsgInfo msgInfo = ackBatch.get(msg.rootId);
            List<Object> ackerTuple;
            if (msgInfo == null) {
                TupleInfo info = new TupleInfo();
                info.setStream(msg.streamId);
                info.setValues(msg.values);
                info.setMessageId(msg.messageId);
                info.setTimestamp(System.currentTimeMillis());

                pending.putHead(msg.rootId, info);

                ackerTuple = JStormUtils.mk_list((Object) msg.rootId, JStormUtils.bit_xor_vals(as), task_id);

                msgInfo = new SpoutMsgInfo(Acker.ACKER_INIT_STREAM_ID, ackerTuple, null, null, null, null);
                ackBatch.put(msg.rootId, msgInfo);
            } else {
                ackerTuple = msgInfo.values;
                ackerTuple.set(1, JStormUtils.bit_xor_vals(ackerTuple.get(1), as));
            }
        }

        return msgId;
    }

    private List<MsgInfo> addToBatches(String key, Map<String, List<MsgInfo>> batches, String streamId, List<Object> tuple, Integer outTaskId, Object messageId,
                                       Long rootId, int batchSize, ICollectorCallback callback) {
        List<MsgInfo> batch = batches.get(key);
        if (batch == null) {
            batch = new ArrayList<MsgInfo>();
            batches.put(key, batch);
        }
        batch.add(new SpoutMsgInfo(streamId, tuple, outTaskId, messageId, rootId, callback));
        if (batch.size() > batchSize) {
            List<MsgInfo> ret = batch;
            batches.put(key, null);
            return ret;
        } else {
            return null;
        }
    }

    class SpoutMsgInfo extends MsgInfo {
        public Long rootId;
        public Object messageId;

        public SpoutMsgInfo(String streamId, List<Object> values, Integer outTaskId, Object messageId, Long rootId, ICollectorCallback callback) {
            super(streamId, values, outTaskId, callback);
            this.messageId = messageId;
            this.rootId = rootId;
        }
    }

    @Override
    public void flush() {
        batchCollector.flush();
    }

}

