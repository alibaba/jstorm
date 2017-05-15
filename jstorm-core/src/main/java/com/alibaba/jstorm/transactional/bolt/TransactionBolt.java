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
package com.alibaba.jstorm.transactional.bolt;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IProtoBatchBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.TaskReceiver;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.BatchCache;
import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.PendingBatch;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.transactional.utils.CountValue;
import com.esotericsoftware.kryo.io.Input;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBolt implements IProtoBatchBolt {
    private static final long serialVersionUID = 7839725373060318309L;

    private static Logger LOG = LoggerFactory.getLogger(TransactionBolt.class);

    protected Map conf;
    protected TopologyContext topologyContext;
    protected static StormTopology sysTopology = null;
    protected String topologyId;
    protected int taskId;
    protected String componentId;
    protected Set<String> upstreamComponents = new HashSet<String>();
    protected Set<Integer> upstreamTasks = new HashSet<Integer>();
    protected Set<String> downstreamComponents = new HashSet<String>();
    protected Set<Integer> downstreamTasks = new HashSet<Integer>();
    protected int topologyMasterId;
    protected boolean isEndBolt = false;

    protected ITransactionBoltExecutor boltExecutor;
    protected TransactionOutputCollector outputCollector;

    protected boolean init = false;
    protected volatile State boltStatus;
    // Information of current in progress batches
    protected BatchTracker currentBatchTracker;
    protected volatile long lastSuccessfulBatch;
    protected Map<Long, BatchTracker> processingBatches;
    protected BatchCache batchCache; // cache for the messages of inter-worker
    protected BatchCache intraBatchCache; // cache for the messages of intra-worker

    protected SerializationFactory.IdDictionary streamIds;
    protected Input kryoInput;
    protected Set<Integer> inputStreamIds;

    public static class BatchTracker {
        private long batchId;
        private TransactionState state;

        private Set<Integer> expectedReceivedSnapshot = new HashSet<>();
        private int expectedTupleCount;
        private int receivedTupleCount;

        public HashMap<Integer, CountValue> sendMsgCount = new HashMap<>();

        public BatchTracker(long id, Set<Integer> upstreamTasks, Set<Integer> downstreamTasks) {
            setBatchId(id);
            expectedReceivedSnapshot.addAll(upstreamTasks);
            expectedTupleCount = 0;
            receivedTupleCount = 0;
            for (Integer task : downstreamTasks) {
                this.sendMsgCount.put(task, new CountValue());
            }
            state = new TransactionState(batchId, null, null);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("batchId=" + batchId);
            sb.append(", expectedReceivedSnapshot=" + expectedReceivedSnapshot);
            sb.append(", expectedTupleCount=" + expectedTupleCount);
            sb.append(", receivedTupleCount=" + receivedTupleCount);
            sb.append(", sendMsgCount=" + sendMsgCount);
            sb.append(", state=" + state);
            return sb.toString();
        }

        public void setBatchId(long id) {
            batchId = id;
        }

        public long getBatchId() {
            return batchId;
        }

        public boolean isAllBarriersReceived() {
            return expectedReceivedSnapshot.size() == 0;
        }

        public boolean receiveBarrier(int sourceTaskId) {
            return expectedReceivedSnapshot.remove(sourceTaskId);
        }

        public TransactionState getState() {
            return state;
        }

        public boolean checkFinish() {
            return isAllBarriersReceived() && (expectedTupleCount == receivedTupleCount);
        }

        public void incrementReceivedCount() {
            receivedTupleCount++;
        }

        public void incrementReceivedCount(int count) {
            receivedTupleCount += count;
        }
    }

    public TransactionBolt(ITransactionBoltExecutor boltExecutor) {
        this.boltExecutor = boltExecutor;
    }

    public ITransactionBoltExecutor getBoltExecutor() {
        return this.boltExecutor;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.topologyContext = context;
        this.topologyId = topologyContext.getTopologyId();
        this.taskId = topologyContext.getThisTaskId();
        this.componentId = topologyContext.getThisComponentId();
        this.topologyMasterId = context.getTopologyMasterId();

        this.outputCollector = new TransactionOutputCollector(this, collector);
        this.boltExecutor.prepare(conf, context, new OutputCollector(outputCollector));

        if (sysTopology == null) {
            try {
                sysTopology = Common.system_topology(stormConf, context.getRawTopology());
            } catch (InvalidTopologyException e) {
                LOG.error("Failed to build system topology", e);
                throw new RuntimeException(e);
            }
        }
        this.lastSuccessfulBatch = TransactionCommon.INIT_BATCH_ID;
        this.processingBatches = new HashMap<>();

        // Get upstream components
        upstreamComponents = TransactionCommon.getUpstreamComponents(componentId, topologyContext);
        upstreamTasks.addAll(new HashSet<Integer>(context.getComponentsTasks(upstreamComponents)));
        // Get downstream components which are belong to this group
        downstreamComponents = TransactionCommon.getDownstreamComponents(componentId, topologyContext.getRawTopology());
        downstreamTasks.addAll(new HashSet<Integer>(context.getComponentsTasks(downstreamComponents)));

        boltStatus = State.INIT;

        this.batchCache = new BatchCache(context, sysTopology, false);
        this.intraBatchCache = new BatchCache(context, sysTopology, true);
        LOG.info("TransactionBolt: upstreamComponents=" + upstreamComponents + ", downstreamComponents=" + downstreamComponents);

        this.kryoInput = new Input(1);
        this.streamIds = new SerializationFactory.IdDictionary(sysTopology);
        this.inputStreamIds = new HashSet<>();
        Set<GlobalStreamId> inputs = topologyContext.getThisSources().keySet();
        for (GlobalStreamId stream : inputs) {
            inputStreamIds.add(streamIds.getStreamId(stream.get_componentId(), stream.get_streamId()));
        }
        Set<String> upstreamComponents = TransactionCommon.getUpstreamComponents(componentId, topologyContext);
        for (String upstreamComponentId : upstreamComponents) {
            inputStreamIds.add(streamIds.getStreamId(upstreamComponentId, TransactionCommon.BARRIER_STREAM_ID));
        }
        // LOG.info("Stream info prepare: streamIds={}, inputStreams={}, inputStreamIds={}", streamIds, inputs, inputStreamIds);

        startInitState();
        init = true;
    }

    @Override
    public void execute(Tuple input) {
        String stream = input.getSourceStreamId();
        try {
            if (stream.equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
                processTransactionEvent(input);
            } else {
                long batchId = ((TupleExt) input).getBatchId();
                if (batchId != -1) {
                    // process batch event
                    if (boltStatus.equals(State.INIT)) {
                        /**
                         *  If bolt has not finished initialization, just cache the incoming batch.
                         *  The cache batches will be processed when bolt becomes active.
                         */
                        intraBatchCache.cachePendingBatch(batchId, input);
                    } else if (isDiscarded(batchId)) {
                        LOG.debug("Tuple was discarded. {}", input);
                    } else if (intraBatchCache.isPendingBatch(batchId, lastSuccessfulBatch)) {
                        intraBatchCache.cachePendingBatch(batchId, input);
                    } else {
                        currentBatchTracker = getProcessingBatchTracker(batchId, true);
                        outputCollector.setCurrBatchTracker(currentBatchTracker);
                        processBatchTuple(input);
                    }
                } else {
                    boltExecutor.execute(input);
                }
            }
        } catch (Exception e) {
            LOG.info("Failed to process input: " + input, e);
            if (!stream.equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
                try {
                    long batchId = ((TupleExt) input).getBatchId();
                    if (batchId != -1)
                        fail(batchId);
                } catch (Exception e1) {
                    LOG.error("Failed to fail batch", e1);
                }
            }
            throw new RuntimeException(e);
        }
    }

    protected void processTransactionEvent(Tuple input) {
        TopoMasterCtrlEvent event = (TopoMasterCtrlEvent) input.getValues().get(0);
        TransactionState state;
        switch (event.getEventType()) {
        case transactionInitState:
            state = (TransactionState) event.getEventValue().get(0);
            boltStatus = State.ACTIVE;
            initState(state);
            // process the pending caches and remove expired ones
            LOG.info("Pending batches when bolt init: {}", intraBatchCache);
            processNextPendingBatch();
            intraBatchCache.removeExpiredBatches(lastSuccessfulBatch);
            break;
        case transactionRollback:
            state = (TransactionState) event.getEventValue().get(0);
            boltStatus = State.ROLLBACK;
            rollback(state);
            break;
        case transactionCommit:
            ackCommit(event.getEventValue());
            break;
        default:
            LOG.warn("Received unexpected transaction event, {}" + event.toString());
            break;
        }
    }

    protected void processBatch(long batchId, PendingBatch batch) {
        currentBatchTracker = getProcessingBatchTracker(batchId, true);
        outputCollector.setCurrBatchTracker(currentBatchTracker);
        List<byte[]> datas = batch.getTuples();
        while (datas != null) {
            for (byte[] data : datas) {
                processBatchTuple(intraBatchCache.getPendingTuple(data));
            }
            datas = batch.getTuples();
        }
    }

    protected void processBatchTuple(Tuple batchEvent) {
        String stream = batchEvent.getSourceStreamId();
        if (stream.equals(TransactionCommon.BARRIER_STREAM_ID)) {
            Pair<MessageId, List<Object>> val = (Pair<MessageId, List<Object>>) batchEvent.getValue(0);
            BatchSnapshot snapshot = (BatchSnapshot) val.getSecond().get(0);
            if (currentBatchTracker.receiveBarrier(batchEvent.getSourceTask()))
                currentBatchTracker.expectedTupleCount += snapshot.getTupleCount();
            else
                LOG.warn("Received expired or unexpected barrier={} from task-{}", snapshot, batchEvent.getSourceTask());
            // LOG.debug("Received batch, stream={}, batchId={}, sourceTask={}, values={}", stream, currentBatchTracker.bactchId,
            // batchEvent.getSourceTask(), snapshot);
            LOG.debug("currentBatchTracker={}\n  processingBatches={}\n  PendingBatches={}\n  intraPendingBatches={}", currentBatchTracker, processingBatches,
                    batchCache, intraBatchCache);

            /*
             * if (currentBatchTracker.isAllBarriersReceived()) { if (currentBatchTracker.checkFinish()) finishCurrentBatch(); else
             * outputCollector.fail(batchEvent); }
             */
        } else {
            for (Object value : batchEvent.getValues()) {
                Pair<MessageId, List<Object>> val = (Pair<MessageId, List<Object>>) value;
                TupleImplExt tuple = new TupleImplExt(topologyContext, val.getSecond(), val.getFirst(), ((TupleImplExt) batchEvent));
                boltExecutor.execute(tuple);
            }
            currentBatchTracker.incrementReceivedCount(batchEvent.getValues().size());
        }
        if (currentBatchTracker.checkFinish()) {
            finishCurrentBatch();
        }
    }

    protected void processNextPendingBatch() {
        PendingBatch nextBatch = intraBatchCache.getNextPendingBatch(lastSuccessfulBatch);
        if (nextBatch != null) {
            long nextBatchId = lastSuccessfulBatch + 1;
            //LOG.info("Get pending batch-{} which is going to be processed.", nextBatchId);
            processBatch(nextBatchId, nextBatch);
        }
    }

    @Override
    public void cleanup() {
        boltExecutor.cleanup();
        batchCache.cleanup();
        intraBatchCache.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        boltExecutor.declareOutputFields(declarer);
        Map<String, StreamInfo> streams = ((OutputFieldsGetter) declarer).getFieldsDeclaration();
        if (streams.size() > 0) {
            declarer.declareStream(TransactionCommon.BARRIER_STREAM_ID, new Fields(TransactionCommon.BARRIER_SNAPSHOT_FIELD));
        } else {
            isEndBolt = true;
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return boltExecutor.getComponentConfiguration();
    }

    private BatchTracker getProcessingBatchTracker(long batchId, boolean createIfNotExsit) {
        BatchTracker tracker = processingBatches.get(batchId);
        if (tracker == null && createIfNotExsit) {
            tracker = new BatchTracker(batchId, upstreamTasks, downstreamTasks);
            processingBatches.put(batchId, tracker);
        }
        return tracker;
    }

    /**
     * Check if the incoming batch would be discarded. 
     * Generally, the incoming batch shall be discarded while bolt is under rollback, or it is expired. 
     * @param batchId
     * @return 
     */
    protected boolean isDiscarded(long batchId) {
        if (batchId == TransactionCommon.INIT_BATCH_ID) {
            return false;
        } else if (!boltStatus.equals(State.ACTIVE)) {
            LOG.debug("Bolt is not active, state={}, tuple is going to be discarded", boltStatus.toString());
            return true;
        } else if (batchId <= lastSuccessfulBatch) {
            LOG.info("Received expired event of batch-{}, current batchId={}", batchId, lastSuccessfulBatch);
            return true;
        } else {
            return false;
        }
    }

    protected void startInitState() {
        LOG.info("Start to retrieve initialize state from topology master");
        // retrieve state from topology master
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionInitState);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    protected void initState(TransactionState state) {
        lastSuccessfulBatch = state.getCurrBatchId();
        LOG.info("Init: state={}, lastSuccessfulBatch={}", state, lastSuccessfulBatch);
    }

    protected void commit() {
        if (boltExecutor instanceof IRichTransactionBoltExecutor) {
            ((IRichTransactionBoltExecutor) boltExecutor).finishBatch(currentBatchTracker.getBatchId());
        }
    }

    protected void ackCommit(List<Object> value) {
        // Do nothing for non-stateful bolt
    }

    protected void rollback(TransactionState state) {
        long batchId = state.getCurrBatchId();
        LOG.info("Start to rollback to batch-{}.\n  currentBatchStateus: {}", batchId, currentBatchStatusInfo());
        lastSuccessfulBatch = batchId;
    }

    protected void cleanupBuffer() {
        if (currentBatchTracker != null) {
            currentBatchTracker = null;
        }
        processingBatches.clear();
        batchCache.cleanup();
        intraBatchCache.cleanup();
        LOG.debug("cleanupBuffer: processingBatches={}\n  pendingBatches={}\n  intraPendingBatches={}\n  currentBatchTracker={}", processingBatches,
                batchCache, intraBatchCache, currentBatchTracker);
    }

    private void ackBatch(long batchId) {
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionAck);
        event.addEventValue(batchId);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
        outputCollector.flush();
    }

    private void removeProcessingBatchTracker(long batchId) {
        processingBatches.remove(batchId);
    }

    private void finishCurrentBatch() {
        long currbatchId = currentBatchTracker.batchId;
        if (currbatchId == TransactionCommon.INIT_BATCH_ID) {
            LOG.info("Received all init events");
            cleanupBuffer();
            boltStatus = State.ACTIVE;
        } else {
            commit();
            lastSuccessfulBatch = currbatchId;
        }

        if (downstreamTasks.size() == 0) {
            ackBatch(currbatchId);
        } else {
            outputCollector.flushBarrier();
        }

        removeProcessingBatchTracker(currbatchId);
        currentBatchTracker = null;
        // LOG.debug("finishCurrentBatch, {}", currentBatchStatusInfo());

        processNextPendingBatch();
    }

    public void fail(long id) {
        // If any failure, send rollback request to TM
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionRollback);
        event.addEventValue(id);
        outputCollector.emitDirectCtrl(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    @Override
    public void protoExecute(TaskReceiver receiver, KryoTupleDeserializer deserializer, DisruptorQueue queue, byte[] data) {
        boolean isCached = false;
        if (!init || (batchCache != null && !batchCache.isExactlyOnceMode())) {
            // If bolt has not finished initialization or was not exactly once mode, just process the tuple immediately
        } else {
            PendingBatch batch = batchCache.getNextPendingBatch(lastSuccessfulBatch);
            if (batch != null) {
                // If there are any pending batches, process them first.
                List<byte[]> pendingMsgs = batch.getTuples();
                while (pendingMsgs != null) {
                    for (byte[] msg : pendingMsgs) {
                        receiver.deserializeTuple(deserializer, msg, queue);
                    }
                    pendingMsgs = batch.getTuples();
                }
            }

            kryoInput.setBuffer(data);
            kryoInput.setPosition(13); // Skip targetTaskId, timeStamp, isBatch
            kryoInput.readInt(true); // Skip sourceTaskId
            int streamId = kryoInput.readInt(true);
            // LOG.info("ProtoExecute: receive init msg from task-{} on stream-{}, data={}", sourceTaskId, streamId, JStormUtils.toPrintableString(data));
            if (inputStreamIds.contains(streamId)) {
                // Read BatchId
                long batchId = kryoInput.readLong(true);
                // LOG.info("ProtoExecute: receive msg for batchId-{}", batchId);
                if (batchCache.isPendingBatch(batchId, lastSuccessfulBatch)) {
                    // LOG.info("Cache batch-{}", batchId);
                    isCached = true;
                    batchCache.cachePendingBatch(batchId, data);
                }
            }
        }

        if (!isCached)
            receiver.deserializeTuple(deserializer, data, queue);
    }

    protected String currentBatchStatusInfo() {
        // Because of the concurrent problem here, just catch the exception as temporary solution.
        try {
            return "processingBatches=" + processingBatches + "\n  pendingBatches=" + batchCache + "\n  intraPendingBatches=" + intraBatchCache
                    + "\n  lastSuccessfulBatch=" + lastSuccessfulBatch;
        } catch (Exception e) {
            LOG.warn("Failed to get current batch status", e);
            return "";
        }
    }
}