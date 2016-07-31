package com.alibaba.jstorm.task.execute;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xiaojian.fxj
 * @since 2.1.1
 */
public class BoltBatchCollector extends BoltCollector {
    private static Logger LOG = LoggerFactory.getLogger(BoltBatchCollector.class);
    private BatchCollector batchCollector;

    private final RotatingMap<Tuple, Integer> pendingTuples = new RotatingMap<Tuple, Integer>(Acker.TIMEOUT_BUCKET_NUM, true);
    private final BlockingQueue<Tuple> pendingAcks = new LinkedBlockingQueue<Tuple>();

    private class BoltMsgInfo extends MsgInfo {
        public Collection<Tuple> anchors;

        public BoltMsgInfo(String streamId, List<Object> values, Collection<Tuple> anchors, Integer outTaskId, ICollectorCallback callback) {
            super(streamId, values, outTaskId, callback);
            this.anchors = anchors;
        }
    }

    public BoltBatchCollector(Task task, RotatingMap<Tuple, Long> tuple_start_times, int message_timeout_secs) {
        super(task, tuple_start_times, message_timeout_secs);

        String componentId = topologyContext.getThisComponentId();

        batchCollector = new BatchCollector(task_id, componentId, storm_conf) {
            public List<MsgInfo> push(String streamId, List<Object> tuple, Integer outTaskId, Collection<Tuple> anchors, Object messageId, Long rootId,
                                      ICollectorCallback callback) {
                if (anchors != null) {
                    synchronized (pendingTuples) {
                        for (Tuple a : anchors) {
                            Integer pendingCount = pendingTuples.get(a);
                            if (pendingCount == null) {
                                pendingCount = 0;
                            }
                            pendingTuples.put(a, ++pendingCount);
                        }
                    }
                }

                if (outTaskId != null) {
                    synchronized (directBatches) {
                        return addToBatches(outTaskId.toString() + "-" + streamId, directBatches, streamId, tuple, outTaskId, anchors, batchSize, callback);
                    }
                } else {
                    synchronized (streamToBatches) {
                        return addToBatches(streamId, streamToBatches, streamId, tuple, outTaskId, anchors, batchSize, callback);
                    }
                }
            }

            public void flush() {
                List<List<Object>> batches = new ArrayList<List<Object>>();
                synchronized (streamToBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : streamToBatches.entrySet()) {
                        if (entry.getValue() != null && entry.getValue().size() > 0) {
                            batches.add(JStormUtils.mk_list(entry.getKey(), null, entry.getValue()));
                            streamToBatches.put(entry.getKey(), null);
                        }
                    }
                }

                synchronized (directBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : directBatches.entrySet()) {
                        if (entry.getValue() != null && entry.getValue().size() > 0) {
                            String[] strings = entry.getKey().split("-", 2);
                            batches.add(JStormUtils.mk_list(strings[1], strings[0], entry.getValue()));
                            directBatches.put(entry.getKey(), null);
                        }
                    }
                }

                for (List<Object> batch : batches) {
                    sendBatch((String) batch.get(0), (String) batch.get(1), (List<MsgInfo>) batch.get(2));
                }

                int size = pendingAcks.size();
                while (--size >= 0) {
                    Tuple ackTuple = pendingAcks.poll();
                    if (ackTuple != null) {
                        if (!sendAckTuple(ackTuple)) {
                            try {
                                pendingAcks.put(ackTuple);
                            } catch (InterruptedException e) {
                                LOG.warn("Failed to put ackTuple, tuple=" + ackTuple, e);
                            }
                        }
                    }
                }
            }
        };
    }

    @Override
    protected List<Integer> sendBoltMsg(String outStreamId, Collection<Tuple> anchors, List<Object> values, Integer outTaskId, ICollectorCallback callback) {
        java.util.List<Integer> outTasks = new ArrayList<Integer>();
        // LOG.info("spout push message to " + out_stream_id);
        List<MsgInfo> batchTobeFlushed = batchCollector.push(outStreamId, values, outTaskId, anchors, null, null, callback);
        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
            outTasks = sendBatch(outStreamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed);
        }
        return outTasks;
    }

    private List<MsgInfo> addToBatches(String key, Map<String, List<MsgInfo>> batches, String streamId, List<Object> tuple, Integer outTaskId,
                                       Collection<Tuple> anchors, int batchSize, ICollectorCallback callback) {
        List<MsgInfo> batch;
        batch = batches.get(key);
        if (batch == null) {
            batch = new ArrayList<MsgInfo>();
            batches.put(key, batch);
        }
        batch.add(new BoltMsgInfo(streamId, tuple, anchors, outTaskId, callback));
        if (batch.size() > batchSize) {
            batches.put(key, null);
            return batch;
        } else {
            return null;
        }
    }

    public List<Integer> sendBatch(String outStreamId, String outTaskId, List<MsgInfo> batchTobeFlushed) {
        final long start = emitTimer.getTime();
        try {
            List<Integer> ret = new ArrayList<Integer>();
            Map<List<Integer>, List<MsgInfo>> outTasks;

            if (outTaskId != null) {
                outTasks = sendTargets.getBatch(Integer.valueOf(outTaskId), outStreamId, batchTobeFlushed);
            } else {
                outTasks = sendTargets.getBatch(outStreamId, batchTobeFlushed);
            }

            if (outTasks == null || outTasks.size() == 0) {

            } else {
                for (Map.Entry<List<Integer>, List<MsgInfo>> entry : outTasks.entrySet()) {
                    List<Integer> tasks = entry.getKey();
                    List<MsgInfo> batch = entry.getValue();

                    for (Integer t : tasks) {
                        BatchTuple batchTuple = new BatchTuple(t, batch.size());
                        for (MsgInfo msg : batch) {
                            Collection<Tuple> as = ((BoltMsgInfo) msg).anchors;
                            MessageId msgId = getMessageId(as);
                            TupleImplExt tp = new TupleImplExt(topologyContext, msg.values, task_id, msg.streamId, msgId);
                            tp.setTargetTaskId(t);
                            batchTuple.addToBatch(tp);
                        }
                        taskTransfer.transfer(batchTuple);
                        //LOG.info("Send batch to task-" + t + ", batchTuple=" + batchTuple.toString());
                    }

                    for (MsgInfo msg : batch) {
                        if (msg.callback != null) {
                            msg.callback.execute(tasks);
                        }
                    }

                }
            }

            for (MsgInfo msg : batchTobeFlushed) {
                Collection<Tuple> anchors = ((BoltMsgInfo) msg).anchors;
                if (anchors != null && anchors.size() > 0) {
                    for (Tuple a : anchors) {
                        synchronized (pendingTuples) {
                            Integer pendingCount = pendingTuples.get(a);
                            if (pendingCount != null) {
                                if (--pendingCount <= 0) {
                                    pendingTuples.remove(a);
                                } else {
                                    pendingTuples.put(a, pendingCount);
                                }
                            }
                        }
                    }
                }
            }

            return ret;
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            emitTimer.updateTime(start);
        }
        return new ArrayList<Integer>();
    }


    @Override
    public void ack(Tuple input) {
        if (ackerNum > 0) {
            if (!sendAckTuple(input)) {
                pendingAcks.add(input);
            }
        }

        Long latencyStart = (Long) tuple_start_times.remove(input);
        if (latencyStart != null && JStormMetrics.enabled) {
            long endTime = System.currentTimeMillis();
            long lifeCycleStart = ((TupleExt) input).getCreationTimeStamp();
            task_stats.bolt_acked_tuple(
                    input.getSourceComponent(), input.getSourceStreamId(), latencyStart, lifeCycleStart, endTime);
        }
    }

    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (ackerNum > 0) {
            pending_acks.remove(input);
            for (Map.Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                List<Object> ackTuple = JStormUtils.mk_list((Object) e.getKey());
                sendBoltMsg(Acker.ACKER_FAIL_STREAM_ID, null, ackTuple, null, null);
            }
        }

        task_stats.bolt_failed_tuple(input.getSourceComponent(), input.getSourceStreamId());
    }

    @Override
    protected MessageId getMessageId(Collection<Tuple> anchors) {
        Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
        if (anchors != null) {
            long now = System.currentTimeMillis();
            if (now - lastRotate > rotateTime) {
                pending_acks.rotate();
                synchronized (pendingTuples) {
                    pendingTuples.rotate();
                }
                lastRotate = now;
            }
            for (Tuple a : anchors) {
                // Long edge_id = MessageId.generateId();
                Long edge_id = MessageId.generateId(random);
                synchronized (pending_acks) {
                    put_xor(pending_acks, a, edge_id);
                }
                for (Long root_id : a.getMessageId().getAnchorsToIds().keySet()) {
                    put_xor(anchors_to_ids, root_id, edge_id);
                }
            }
        }
        return MessageId.makeId(anchors_to_ids);
    }

    private boolean sendAckTuple(Tuple input) {
        boolean ret = false;
        Integer pendingCount;
        synchronized (pendingTuples) {
            pendingCount = pendingTuples.get(input);
        }
        if (pendingCount == null || pendingCount <= 0) {
            long ack_val = 0L;
            Object pend_val = pending_acks.remove(input);
            if (pend_val != null) {
                ack_val = (Long) (pend_val);
            }

            for (Map.Entry<Long, Long> e : input.getMessageId().getAnchorsToIds().entrySet()) {
                List<Object> ackTuple = JStormUtils.mk_list((Object) e.getKey(), JStormUtils.bit_xor(e.getValue(), ack_val));
                sendBoltMsg(Acker.ACKER_ACK_STREAM_ID, null, ackTuple, null, null);
            }
            ret = true;
            //LOG.info("Acked tuple=" + input);
        }

        return ret;
    }

    @Override
    public void flush() {
        batchCollector.flush();
    }

    @Override
    void transferCtr(TupleImplExt tupleExt) {
        int taskId = tupleExt.getTargetTaskId();
        BatchTuple batchTuple = new BatchTuple(taskId, 1);
        batchTuple.addToBatch(tupleExt);
        taskTransfer.transferControl(batchTuple);
    }

    @Override
    void unanchoredSend(TopologyContext topologyContext, TaskSendTargets taskTargets, TaskTransfer transfer_fn, String stream, List<Object> values) {
        UnanchoredSend.sendBatch(topologyContext, taskTargets, transfer_fn, stream, values);
    }
}
