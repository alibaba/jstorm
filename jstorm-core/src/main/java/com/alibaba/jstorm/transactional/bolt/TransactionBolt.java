package com.alibaba.jstorm.transactional.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.ctrlevent.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.transactional.BatchCache;
import com.alibaba.jstorm.transactional.BatchGroupId;
import com.alibaba.jstorm.transactional.BatchSnapshot;
import com.alibaba.jstorm.transactional.TransactionCommon;
import com.alibaba.jstorm.transactional.TransactionOutputFieldsDeclarer;
import com.alibaba.jstorm.transactional.state.TransactionState.State;
import com.alibaba.jstorm.transactional.state.TransactionState;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.esotericsoftware.kryo.io.Input;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IProtoBatchBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;

public class TransactionBolt implements IProtoBatchBolt {
    private static final long serialVersionUID = 7839725373060318309L;

    public static Logger LOG = LoggerFactory.getLogger(TransactionBolt.class);

    protected Map conf;
    protected TopologyContext topologyContext;
    protected static StormTopology sysTopology = null;
    protected String topologyId;
    protected int taskId;
    protected String componentId;
    protected Set<Integer> upstreamTasks;
    protected Set<Integer> downstreamTasks;
    protected int topologyMasterId;
    protected boolean isEndBolt = false;

    protected ITransactionBoltExecutor boltExecutor;
    protected TransactionOutputCollector outputCollector;

    protected volatile State boltStatus;
    // Information of current in progress batches
    protected BatchTracker currentBatchTracker;
    protected ConcurrentHashMap<Integer, Long> lastSuccessfulBatch;
    protected HashMap<Integer, Map<Long, BatchTracker>> processingBatches;
    protected BatchCache batchCache;

    protected SerializationFactory.IdDictionary streamIds;
    protected Input kryoInput;
    protected Set<Integer> inputStreamIds;

    public static class BatchTracker {
        private BatchGroupId bactchGroupId = new BatchGroupId();
        private TransactionState state;

        private Set<Integer> expectedReceivedSnapshot = new HashSet<Integer>();
        private int expectedTupleCount;
        private int receivedTupleCount;

        public HashMap<Integer, CountValue> sendMsgCount = new HashMap<Integer, CountValue>();

        public BatchTracker(BatchGroupId id, Set<Integer> upstreamTasks, Set<Integer> downstreamTasks) {
            setBatchGroupId(id);
            expectedReceivedSnapshot.addAll(upstreamTasks);
            expectedTupleCount = 0;
            receivedTupleCount = 0;
            for (Integer task : downstreamTasks) {
                this.sendMsgCount.put(task, new CountValue());
            }
            state = new TransactionState(id.groupId, id.batchId, null, null);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("bactchGroupId=" + bactchGroupId);
            sb.append(", expectedReceivedSnapshot=" + expectedReceivedSnapshot);
            sb.append(", expectedTupleCount=" + expectedTupleCount);
            sb.append(", receivedTupleCount=" + receivedTupleCount);
            sb.append(", sendMsgCount=" + sendMsgCount);
            sb.append(", state=" + state);
            return sb.toString();
        }

        public boolean isBatchInprogress() {
            return bactchGroupId.groupId != 0;
        }

        public void setBatchGroupId(BatchGroupId id) {
            this.bactchGroupId.setBatchGroupId(id);;
        }

        public BatchGroupId getBatchGroupId() {
            return bactchGroupId;
        }

        public boolean isAllBarriersReceived() {
            return expectedReceivedSnapshot.size() == 0;
        }

        public void receiveBarrier(int sourceTaskId) {
            expectedReceivedSnapshot.remove(sourceTaskId);
        }

        public TransactionState getState() {
            return state;
        }

        public boolean checkFinish() {
            boolean ret = true;
            if (isAllBarriersReceived()) {
                ret = (expectedTupleCount == receivedTupleCount);
            } else {
                ret = false;
            }

            return ret;
        }

        public void incrementReceivedCount() {
            receivedTupleCount++;
        }

        public void incrementReceivedCount(int count) {
            receivedTupleCount += count;
        }
    }

    public static class CountValue {
        public int count = 0;

        public CountValue() {
            
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CountValue) {
                return ((CountValue) obj).count == count;
            } else {
                return false; 
            }
        }

        @Override
        public String toString() {
            return String.valueOf(count);
        }
    }

    public TransactionBolt(ITransactionBoltExecutor boltExecutor) {
        this.boltExecutor = boltExecutor;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.topologyContext = context;
        this.topologyId = topologyContext.getTopologyId();
        this.taskId = topologyContext.getThisTaskId();
        this.componentId = topologyContext.getThisComponentId();
        this.upstreamTasks = TransactionCommon.getUpstreamTasks(componentId, topologyContext);
        this.downstreamTasks = TransactionCommon.getDownstreamTasks(componentId, topologyContext);
        this.topologyMasterId = context.getTopologyMasterId();
        LOG.info("TransactionBolt: upstreamTasks=" + upstreamTasks + ", downstreamTasks=" + downstreamTasks);

        this.outputCollector = new TransactionOutputCollector(this, collector);
        this.boltExecutor.prepare(conf, context, new OutputCollector(outputCollector));

        this.boltStatus = State.INIT;
        if (sysTopology == null) {
            try {
                sysTopology = Common.system_topology(stormConf, context.getRawTopology());
            } catch (InvalidTopologyException e) {
                LOG.error("Failed to build system topology", e);
                throw new RuntimeException(e);
            } 
        }
        this.lastSuccessfulBatch = new ConcurrentHashMap<Integer, Long>();
        this.processingBatches = new HashMap<Integer, Map<Long, BatchTracker>>();
        Set<String> upstreamSpoutNames = TransactionCommon.getUpstreamSpouts(componentId, topologyContext);
        for (String spoutName : upstreamSpoutNames) {
            int groupId = TransactionCommon.groupIndex(topologyContext.getRawTopology(), spoutName);
            lastSuccessfulBatch.put(groupId, TransactionCommon.INIT_BATCH_ID);
            processingBatches.put(groupId, new HashMap<Long, BatchTracker>());
        }
        this.batchCache = new BatchCache(context, upstreamSpoutNames, sysTopology);

        this.kryoInput = new Input(1);
        this.streamIds = new SerializationFactory.IdDictionary(sysTopology);
        this.inputStreamIds = new HashSet<Integer>();
        Set<GlobalStreamId> inputs = topologyContext.getThisSources().keySet();
        for (GlobalStreamId stream : inputs) {
            inputStreamIds.add(streamIds.getStreamId(stream.get_componentId(), stream.get_streamId()));
        }
        for (String upstreamComponentId : TransactionCommon.getUpstreamComponents(componentId, topologyContext)) {
            inputStreamIds.add(streamIds.getStreamId(upstreamComponentId, TransactionCommon.BARRIER_STREAM_ID));
        }
        //LOG.info("Stream info prepare: streamIds={}, inputStreams={}, inputStreamIds={}", streamIds, inputs, inputStreamIds);

        startInitState();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String stream = input.getSourceStreamId();
            if (stream.equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
                processTransactionEvent(input);
            } else {
                List<Object> firstTupleValue = ((Pair<MessageId, List<Object>>) input.getValues().get(0)).getSecond();
                BatchGroupId batchGroupId = (BatchGroupId) firstTupleValue.get(0);
                if (isDiscarded(batchGroupId)) {
                    LOG.debug("Tuple was discarded. {}", input);
                    return;
                } else if (batchCache.isPendingBatch(batchGroupId, lastSuccessfulBatch)) {
                    if (batchCache.cachePendingBatch(batchGroupId, input, lastSuccessfulBatch)) {
                        return;
                    }
                }

                currentBatchTracker = getProcessingBatch(batchGroupId, true);
                outputCollector.setCurrBatchTracker(currentBatchTracker);
                processBatchTuple(input);
            }
        } catch (Exception e) {
            LOG.info("Failed to process input={}", input, e);
            throw new RuntimeException(e);
        }
    }

    protected void processTransactionEvent(Tuple input) {
        TopoMasterCtrlEvent event = (TopoMasterCtrlEvent) input.getValues().get(0);
        TransactionState state = null;
        switch (event.getEventType()) {
            case transactionInitState:
                state = (TransactionState) event.getEventValue().get(0);
                initState(state);
                boltStatus = State.ACTIVE;
                break;
            case transactionRollback:
                boltStatus = State.ROLLBACK;
                state = (TransactionState) event.getEventValue().get(0);
                rollback(state);
                break;
            case transactionCommit:
                BatchGroupId batchGroupId = (BatchGroupId) event.getEventValue().get(0);
                ackCommit(batchGroupId);
                break;
            default:
                LOG.warn("Received unexpected transaction event, {}" + event.toString());
                break;
        }
    }

    public void processBatch(BatchGroupId batchGroupId, List<Tuple> batch) {
        currentBatchTracker = getProcessingBatch(batchGroupId, true);
        outputCollector.setCurrBatchTracker(currentBatchTracker);
        for (Tuple batchTuple : batch) {
            processBatchTuple(batchTuple);
        }
    }

    public void processBatchTuple(Tuple batchEvent) {
        String stream = batchEvent.getSourceStreamId();
        if (stream.equals(TransactionCommon.BARRIER_STREAM_ID)) {
            Pair<MessageId, List<Object>> val = (Pair<MessageId, List<Object>>) batchEvent.getValue(0);
            BatchSnapshot snapshot = (BatchSnapshot) val.getSecond().get(1);
            currentBatchTracker.receiveBarrier(batchEvent.getSourceTask());
            currentBatchTracker.expectedTupleCount += snapshot.getTupleCount();
            LOG.debug("Received batch, stream={}, batchGroupId={}, sourceTask={}, values={}", stream, currentBatchTracker.bactchGroupId, batchEvent.getSourceTask(), snapshot);
            LOG.debug("currentBatchTracker={}, processingBatches={}, pendingBatches={}", currentBatchTracker, processingBatches, batchCache);
        } else {
            for (Object value : batchEvent.getValues()) {
                /*List<Object> firstTupleValue = ((Pair<MessageId, List<Object>>) value).getSecond();
                BatchGroupId batchGroupId = (BatchGroupId) firstTupleValue.get(0);
                if (!batchGroupId.equals(currentBatchTracker.bactchGroupId)) {
                    LOG.warn("batchgroupid-{} is not equal to the once of current batch tracker-{}!", batchGroupId, currentBatchTracker.bactchGroupId);
                }*/

                Pair<MessageId, List<Object>> val = (Pair<MessageId, List<Object>>) value;
                val.getSecond().remove(0);
                TupleImplExt tuple = new TupleImplExt(topologyContext, val.getSecond(), val.getFirst(), ((TupleImplExt) batchEvent));
                boltExecutor.execute(tuple);
            }
            currentBatchTracker.incrementReceivedCount(batchEvent.getValues().size());
        }
        if (currentBatchTracker.checkFinish()) {
            finishCurrentBatch();
        }
    }

    @Override
    public void cleanup() {
        boltExecutor.cleanup();
        batchCache.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        TransactionOutputFieldsDeclarer transactionDeclarer = new TransactionOutputFieldsDeclarer();
        boltExecutor.declareOutputFields(transactionDeclarer);
        Map<String, StreamInfo> streams = transactionDeclarer.getFieldsDeclaration();
        for (Entry<String, StreamInfo> entry : streams.entrySet()) {
            String streamName = entry.getKey();
            StreamInfo streamInfo = entry.getValue();
            declarer.declareStream(streamName, streamInfo.is_direct(), new Fields(streamInfo.get_output_fields()));
        }

        if (streams.size() > 0) {
            declarer.declareStream(TransactionCommon.BARRIER_STREAM_ID, new Fields(TransactionCommon.BATCH_GROUP_ID_FIELD, TransactionCommon.BARRIER_SNAPSHOT_FIELD));
        } else {
            isEndBolt = true;
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return boltExecutor.getComponentConfiguration();
    }

    private BatchTracker getProcessingBatch(BatchGroupId batchGroupId, boolean createIfNotExsit) {
        BatchTracker tracker = null;
        Map<Long, BatchTracker> batches = processingBatches.get(batchGroupId.groupId);
        if (batches != null) {
            tracker = batches.get(batchGroupId.batchId);
            if (tracker == null && createIfNotExsit) {
                tracker = new BatchTracker(batchGroupId, upstreamTasks, downstreamTasks);
                batches.put(batchGroupId.batchId, tracker);
            }
        } 
        return tracker;
    }

    protected boolean isDiscarded(BatchGroupId batchGroupId) {
        if (batchGroupId.batchId == TransactionCommon.INIT_BATCH_ID) {
            return false;
        } else if (boltStatus.equals(State.ACTIVE) == false) {
            LOG.debug("Bolt is not active, state={}, tuple is going to be discarded", boltStatus.toString());
            return true;
        } else {
            return false;
        }
    }

    protected void startInitState() {
        LOG.info("Start to retrieve initialize state from topology master");
        // retrieve state from topology master
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionInitState);
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    protected void initState(TransactionState state) {
        BatchGroupId batchGroupId = state.getCurrBatchGroupId();
        lastSuccessfulBatch.put(batchGroupId.groupId, batchGroupId.batchId);
        LOG.info("Init: state={}, lastSuccessfulBatch={}", state, lastSuccessfulBatch);
    }

    protected void commit() {
        // Do nothing for non-stateful bolt
    }

    protected void ackCommit(BatchGroupId batchGroupId) {
        // Do nothing for non-stateful bolt
    }

    protected void rollback(TransactionState state) {
        BatchGroupId batchGroupId = state.getCurrBatchGroupId();
        LOG.info("Start to rollback to batch-{}, currentBatchStateus={}", batchGroupId, currentBatchStatusInfo());
        lastSuccessfulBatch.put(batchGroupId.groupId, batchGroupId.batchId);
        cleanupBuffer(state.getCurrBatchGroupId().groupId);
    }

    protected void cleanupBuffer(int groupId) {
        if (currentBatchTracker != null && currentBatchTracker.getBatchGroupId().groupId == groupId) {
            currentBatchTracker = null;
        }
        Map<Long, BatchTracker> batches = processingBatches.get(groupId);
        batches.clear();
        batchCache.cleanup(groupId);
        LOG.info("cleanupBuffer: processingBatches={}, pendingBatches={}, currentBatchTracker={}", 
                  processingBatches, batchCache, currentBatchTracker);
    }

    private void ackBatch(BatchGroupId batchGroupId) {
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionAck);
        event.addEventValue(batchGroupId);
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    private void removeProcessingBatch(BatchGroupId batchGroupId) {
        Map<Long, BatchTracker> batches = processingBatches.get(batchGroupId.groupId);
        batches.remove(batchGroupId.batchId);
    }

    private void finishCurrentBatch() {
        BatchGroupId batchGroupId = currentBatchTracker.bactchGroupId;
        if (batchGroupId.batchId == TransactionCommon.INIT_BATCH_ID) {
            LOG.info("Received all init events");
            cleanupBuffer(batchGroupId.groupId);
            boltStatus = State.ACTIVE;
        } else {
            commit();
            lastSuccessfulBatch.put(batchGroupId.groupId, batchGroupId.batchId);
        }

        if (downstreamTasks.size() == 0) {
            ackBatch(batchGroupId);
        } else {
            outputCollector.flushBarrier();
        }

        removeProcessingBatch(batchGroupId);
        currentBatchTracker = null;
        LOG.debug("finishCurrentBatch, {}", currentBatchStatusInfo());

        List<Tuple> goingtoProcessBatch = batchCache.getNextPendingTuples(lastSuccessfulBatch);
        if (goingtoProcessBatch != null) {
            BatchGroupId nextGroupBatchId = new BatchGroupId(batchGroupId.groupId, batchGroupId.batchId + 1);
            //LOG.info("Get pending batch-{} which is going to be processed. size={}", nextGroupBatchId, goingtoProcessBatch.size());
            processBatch(nextGroupBatchId, goingtoProcessBatch);
        }
    }

    public void fail(BatchGroupId id) {
        // If any failure, send rollback request to TM 
        TopoMasterCtrlEvent event = new TopoMasterCtrlEvent(EventType.transactionRollback);
        event.addEventValue(id);
        outputCollector.emitDirectByDelegate(topologyMasterId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, null, new Values(event));
    }

    @Override
    public List<byte[]> protoExecute(byte[] data) {
        if (batchCache == null || !batchCache.isExactlyOnceMode()) {
            return null;
        }

        List<byte[]> ret = new ArrayList<byte[]>();
        if (kryoInput == null) {
            // Bolt has not finished initialize
            ret.add(data);
            return ret;
        }
        
        kryoInput.setBuffer(data);
        kryoInput.setPosition(13); // Skip targetTaskId, timeStamp, isBatch
        int sourceTaskId = kryoInput.readInt(true); // Skip sourceTaskId
        int streamId = kryoInput.readInt(true);
        // LOG.info("ProtoExecute: receive init msg from task-{} on stream-{}, data={}", sourceTaskId, streamId, JStormUtils.toPrintableString(data)); 
        if (inputStreamIds.contains(streamId)) {
            kryoInput.readInt(true); // Skip length of batch
            kryoInput.readInt(true); // Skip type of tuple value
            kryoInput.readInt(true); // Skip length of tuple value
            kryoInput.readInt(true); // Skip registration id of BatchGroupId class
            // Read BatchGroupId
            BatchGroupId batchGroupId = new BatchGroupId(kryoInput.readInt(true), kryoInput.readLong(true));
            //LOG.info("ProtoExecute: receive msg for batchGroupId-{}", batchGroupId);
            if (batchGroupId.batchId == TransactionCommon.INIT_BATCH_ID) {
                ret.add(data);
            } else {
                if (batchCache.isPendingBatch(batchGroupId, lastSuccessfulBatch)) {
                    //LOG.info("Cache batch-{}", batchGroupId);
                    if (!batchCache.cachePendingBatch(batchGroupId, data, lastSuccessfulBatch)) {
                        ret.add(data);
                    }
                } else {
                    ret.add(data);
                }
            }
        } else {
            ret.add(data);
        }

        return ret;
    }

    protected String currentBatchStatusInfo() {
        return "processingBatches=" + processingBatches + ", pendingBatches=" + batchCache + ", lastSuccessfulBatch=" + lastSuccessfulBatch;
    }
}