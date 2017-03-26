package com.alibaba.jstorm.task.execute;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
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
import java.util.concurrent.TimeUnit;

/**
 * @author xiaojian.fxj
 * @since 2.1.1
 */
public class BoltBatchCollector extends BoltCollector {
    private static Logger LOG = LoggerFactory.getLogger(BoltBatchCollector.class);
    private BatchCollector batchCollector;
    private int batchSize;
    private CallIntervalGauge timeIntervalGauge;

    private final RotatingMap<Tuple, Integer> pendingTuples = new RotatingMap<Tuple, Integer>(Acker.TIMEOUT_BUCKET_NUM, true);
    private final BlockingQueue<Tuple> pendingAcks = new LinkedBlockingQueue<Tuple>();

    private class BoltMsgInfo extends MsgInfo {
        public Collection<Tuple> anchors;

        public BoltMsgInfo(String streamId, List<Object> values, Collection<Tuple> anchors, Integer outTaskId, ICollectorCallback callback) {
            super(streamId, values, outTaskId, callback);
            this.anchors = anchors;
        }
    }

    private final Map<Integer, Map<String, List<Object>>> pendingSendMsgs = new HashMap<Integer, Map<String, List<Object>>>();

    public BoltBatchCollector(Task task, RotatingMap<Tuple, Long> tuple_start_times, int message_timeout_secs) {
        super(task, tuple_start_times, message_timeout_secs);

        String componentId = topologyContext.getThisComponentId();

        timeIntervalGauge = new CallIntervalGauge();
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(task.getTopologyId(), componentId, task.getTaskId(), MetricDef.TASK_BATCH_INTERVAL_TIME, MetricType.GAUGE),
                new AsmGauge(timeIntervalGauge));

        batchCollector = new BatchCollector(task_id, componentId, storm_conf) {
            public void pushAndSend(String streamId, List<Object> tuple, Integer outTaskId, Collection<Tuple> anchors, Object messageId, Long rootId,
                                      ICollectorCallback callback) {
                if (anchors != null && ackerNum > 0) {
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
                        List<MsgInfo> batchTobeFlushed = addToBatches(outTaskId.toString() + "-" + streamId, 
                                directBatches, streamId, tuple, outTaskId, anchors, batchSize, callback);
                        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
                            timeIntervalGauge.incrementAndGet();
                            sendBatch(streamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed, false);
                        }
                    }
                } else {
                    synchronized (streamToBatches) {
                        List<MsgInfo> batchTobeFlushed = addToBatches(streamId, streamToBatches, streamId, tuple, outTaskId, anchors, batchSize, callback);
                        if (batchTobeFlushed != null && batchTobeFlushed.size() > 0) {
                            timeIntervalGauge.incrementAndGet();
                            sendBatch(streamId, (outTaskId != null ? outTaskId.toString() : null), batchTobeFlushed, false);
                        }
                    }
                }
            }

            public synchronized void flush() {
                synchronized (streamToBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : streamToBatches.entrySet()) {
                        List<MsgInfo> batch = streamToBatches.put(entry.getKey(), null);
                        if (batch != null && batch.size() > 0) {
                            sendBatch(entry.getKey(), null, batch, true);
                        }
                    }
                }

                synchronized (directBatches) {
                    for (Map.Entry<String, List<MsgInfo>> entry : directBatches.entrySet()) {
                        List<MsgInfo> batch = directBatches.put(entry.getKey(), null);
                        if (batch != null && batch.size() > 0) {
                            // taskId-StreamId --> [taskId, streamId]
                            String[] strings = entry.getKey().split("-", 2);
                            sendBatch(strings[1], strings[0], batch, true);
                        }
                    }
                }

                processPendingAcks();
            }

            private void processPendingAcks() {
                int size = pendingAcks.size();
                while (--size >= 0) {
                    Tuple ackTuple = pendingAcks.poll();
                    if (ackTuple != null && sendAckTuple(ackTuple) == false) {
                        try {
                           pendingAcks.put(ackTuple);
                        } catch (InterruptedException e) {
                           LOG.warn("Failed to put ackTuple, tuple=" + ackTuple, e);
                        }
                    }
                }
            }
        };
        
        batchSize = batchCollector.getConfigBatchSize();
    }

    @Override
    protected List<Integer> sendBoltMsg(String outStreamId, Collection<Tuple> anchors, List<Object> values, Integer outTaskId, ICollectorCallback callback) {

        batchCollector.pushAndSend(outStreamId, values, outTaskId, anchors, null, null, callback);
        return null;
    }

    private List<MsgInfo> addToBatches(String key, Map<String, List<MsgInfo>> batches, String streamId, List<Object> tuple, Integer outTaskId,
                                       Collection<Tuple> anchors, int batchSize, ICollectorCallback callback) {
        List<MsgInfo> batch = batches.get(key);
        if (batch == null) {
            batch = new ArrayList<MsgInfo>();
            batches.put(key, batch);
        }
        batch.add(new BoltMsgInfo(streamId, tuple, anchors, outTaskId, callback));
        if (batch.size() > batchSize) {
            List<MsgInfo> ret = batch;
            batches.put(key, null);
            return ret;
        } else {
            return null;
        }
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

    public List<Integer> sendBatch(String outStreamId, String outTaskId, List<MsgInfo> batchTobeFlushed, boolean isFlush) {
        final long start = emitTimer.getTime();
        try {
            Map<Object, List<MsgInfo>> outTasks = null;

            if (outTaskId != null) {
                outTasks = sendTargets.getBatch(Integer.valueOf(outTaskId), outStreamId, batchTobeFlushed);
            } else {
                outTasks = sendTargets.getBatch(outStreamId, batchTobeFlushed);
            }

            if (outTasks == null || outTasks.size() == 0) {
			
            } else {
                for (Map.Entry<Object, List<MsgInfo>> entry : outTasks.entrySet()) {
                    Object target = entry.getKey();
                    List<Integer> tasks = (target instanceof Integer) ? JStormUtils.mk_list((Integer) target) : ((List<Integer>) target);
                    List<MsgInfo> batch = entry.getValue();

                    for (Integer t : tasks) {
                    	List<Object> batchValues = new ArrayList<Object>();
                    	for (MsgInfo msg : batch) {
                    		BoltMsgInfo msgInfo = (BoltMsgInfo) msg;
                    		Pair<MessageId, List<Object>> pair = new Pair<MessageId, List<Object>>(getMessageId(msgInfo.anchors), msgInfo.values);
                    		batchValues.add(pair);
                    	}

                        TupleImplExt batchTuple = new TupleImplExt(topologyContext, batchValues, task_id, outStreamId, null);
                        batchTuple.setTargetTaskId(t);
                        batchTuple.setBatchTuple(true);
                        taskTransfer.transfer(batchTuple);
                    }

                    for (MsgInfo msg : batch) {
                        if (msg.callback != null) {
                            msg.callback.execute(outStreamId, tasks, msg.values);
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

            return null;
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        } finally {
            emitTimer.updateTime(start);
        }
        return new ArrayList<Integer>();
    }


    @Override
    public void ack(Tuple input) {
        if (input.getMessageId() != null) {
            if (!sendAckTuple(input)) {
                pendingAcks.add(input);
            }
        }

        Long latencyStart = (Long) tuple_start_times.remove(input);
        task_stats.bolt_acked_tuple(input.getSourceComponent(), input.getSourceStreamId());
        if (latencyStart != null && JStormMetrics.enabled) {
            long endTime = System.currentTimeMillis();
            task_stats.update_bolt_acked_latency(input.getSourceComponent(), input.getSourceStreamId(), latencyStart, endTime);
        }
    }

    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (input.getMessageId() != null) {
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
        MessageId ret = null;
        if (anchors != null) {
            Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
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
                MessageId messageId = a.getMessageId();
                if (messageId != null){
                    for (Long root_id : messageId.getAnchorsToIds().keySet()) {
                        put_xor(anchors_to_ids, root_id, edge_id);
                    }
                }
            }
            ret = MessageId.makeId(anchors_to_ids);
        }
        return ret;
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
            MessageId messageId = input.getMessageId();
            if (messageId != null){
                for (Map.Entry<Long, Long> e : messageId.getAnchorsToIds().entrySet()) {
                    List<Object> ackTuple = JStormUtils.mk_list((Object) e.getKey(), JStormUtils.bit_xor(e.getValue(), ack_val));
                    sendBoltMsg(Acker.ACKER_ACK_STREAM_ID, null, ackTuple, null, null);
                }
            }
            ret = true;
        }

        return ret;
    }

    @Override
    public void flush() {
        batchCollector.flush();
    }
}
