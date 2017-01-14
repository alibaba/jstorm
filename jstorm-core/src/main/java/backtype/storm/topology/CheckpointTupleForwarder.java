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
package backtype.storm.topology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.spout.CheckPointState;
import backtype.storm.spout.CheckPointState.Action;
import backtype.storm.spout.CheckpointSpout;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Wraps {@link IRichBolt} and forwards checkpoint tuples in a
 * stateful topology.
 * <p>
 * When a storm topology contains one or more {@link IStatefulBolt} all non-stateful
 * bolts are wrapped in {@link CheckpointTupleForwarder} so that the checkpoint tuples
 * can flow through the entire topology DAG.
 * </p>
 */
public class CheckpointTupleForwarder implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTupleForwarder.class);
    private final IRichBolt bolt;
    private final Map<TransactionRequest, Integer> transactionRequestCount;
    private int checkPointInputTaskCount;
    private long lastTxid = Long.MIN_VALUE;
    private AnchoringOutputCollector collector;

    public CheckpointTupleForwarder(IRichBolt bolt) {
        this.bolt = bolt;
        transactionRequestCount = new HashMap<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        init(context, collector);
        bolt.prepare(stormConf, context, this.collector);
    }

    protected void init(TopologyContext context, OutputCollector collector) {
        this.collector = new AnchoringOutputCollector(collector);
        this.checkPointInputTaskCount = getCheckpointInputTaskCount(context);
    }

    @Override
    public void execute(Tuple input) {
        if (CheckpointSpout.isCheckpoint(input)) {
            processCheckpoint(input);
        } else {
            handleTuple(input);
        }
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
        declarer.declareStream(CheckpointSpout.CHECKPOINT_STREAM_ID, new Fields(CheckpointSpout.CHECKPOINT_FIELD_TXID, CheckpointSpout.CHECKPOINT_FIELD_ACTION));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    /**
     * Forwards the checkpoint tuple downstream. Sub-classes can override
     * with the logic for handling checkpoint tuple.
     *
     * @param checkpointTuple  the checkpoint tuple
     * @param action the action (prepare, commit, rollback or initstate)
     * @param txid   the transaction id.
     */
    protected void handleCheckpoint(Tuple checkpointTuple, Action action, long txid) {
        collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
        collector.ack(checkpointTuple);
    }

    /**
     * Hands off tuple to the wrapped bolt to execute. Sub-classes can
     * override the behavior.
     * <p>
     * Right now tuples continue to get forwarded while waiting for checkpoints to arrive on other streams
     * after checkpoint arrives on one of the streams. This can cause duplicates but still at least once.
     * </p>
     *
     * @param input the input tuple
     */
    protected void handleTuple(Tuple input) {
        bolt.execute(input);
    }

    /**
     * Invokes handleCheckpoint once checkpoint tuple is received on
     * all input checkpoint streams to this component.
     */
    private void processCheckpoint(Tuple input) {
        Action action = (Action) input.getValueByField(CheckpointSpout.CHECKPOINT_FIELD_ACTION);
        long txid = input.getLongByField(CheckpointSpout.CHECKPOINT_FIELD_TXID);
        if (shouldProcessTransaction(action, txid)) {
            LOG.debug("Processing action {}, txid {}", action, txid);
            try {
                if (txid >= lastTxid) {
                    handleCheckpoint(input, action, txid);
                    if (action == CheckPointState.Action.ROLLBACK) {
                        lastTxid = txid - 1;
                    } else {
                        lastTxid = txid;
                    }
                } else {
                    LOG.debug("Ignoring old transaction. Action {}, txid {}", action, txid);
                    collector.ack(input);
                }
            } catch (Throwable th) {
                LOG.error("Got error while processing checkpoint tuple", th);
                collector.fail(input);
                collector.reportError(th);
            }
        } else {
            LOG.debug("Waiting for action {}, txid {} from all input tasks. checkPointInputTaskCount {}, " +
                              "transactionRequestCount {}", action, txid, checkPointInputTaskCount, transactionRequestCount);
            collector.ack(input);
        }
    }

    /**
     * returns the total number of input checkpoint streams across
     * all input tasks to this component.
     */
    private int getCheckpointInputTaskCount(TopologyContext context) {
        int count = 0;
        for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
            if (CheckpointSpout.CHECKPOINT_STREAM_ID.equals(inputStream.get_streamId())) {
                count += context.getComponentTasks(inputStream.get_componentId()).size();
            }
        }
        return count;
    }

    /**
     * Checks if check points have been received from all tasks across
     * all input streams to this component
     */
    private boolean shouldProcessTransaction(Action action, long txid) {
        TransactionRequest request = new TransactionRequest(action, txid);
        Integer count;
        if ((count = transactionRequestCount.get(request)) == null) {
            transactionRequestCount.put(request, 1);
            count = 1;
        } else {
            transactionRequestCount.put(request, ++count);
        }
        if (count == checkPointInputTaskCount) {
            transactionRequestCount.remove(request);
            return true;
        }
        return false;
    }

    private static class TransactionRequest {
        private final Action action;
        private final long txid;

        TransactionRequest(Action action, long txid) {
            this.action = action;
            this.txid = txid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionRequest that = (TransactionRequest) o;

            if (txid != that.txid) return false;
            return !(action != null ? !action.equals(that.action) : that.action != null);

        }

        @Override
        public int hashCode() {
            int result = action != null ? action.hashCode() : 0;
            result = 31 * result + (int) (txid ^ (txid >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TransactionRequest{" +
                    "action='" + action + '\'' +
                    ", txid=" + txid +
                    '}';
        }
    }


    protected static class AnchoringOutputCollector extends OutputCollector {
        AnchoringOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {
            throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple) {
            throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
        }

    }

}
