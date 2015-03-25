package storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.WindowedTimeThrottler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.ICommitterTridentSpout;
import storm.trident.topology.state.TransactionalState;

public class MasterBatchCoordinator extends BaseRichSpout { 
    public static final Logger LOG = Logger.getLogger(MasterBatchCoordinator.class);
    
    public static final long INIT_TXID = 1L;
    
    
    public static final String BATCH_STREAM_ID = "$batch";
    public static final String COMMIT_STREAM_ID = "$commit";
    public static final String SUCCESS_STREAM_ID = "$success";

    private static final String CURRENT_TX = "currtx";
    private static final String CURRENT_ATTEMPTS = "currattempts";
    
    private static enum Operation {
        ACK,
        FAIL,
        NEXTTUPLE
    }

    private List<TransactionalState> _states = new ArrayList();
    
    TreeMap<Long, TransactionStatus> _activeTx = new TreeMap<Long, TransactionStatus>();
    TreeMap<Long, Integer> _attemptIds;
    
    private SpoutOutputCollector _collector;
    Long _currTransaction;
    int _maxTransactionActive;
    
    List<ITridentSpout.BatchCoordinator> _coordinators = new ArrayList();
    
    
    List<String> _managedSpoutIds;
    List<ITridentSpout> _spouts;
    WindowedTimeThrottler _throttler;
    
    boolean _active = true;
    
    AtomicBoolean failedOccur = new AtomicBoolean(false);
    
    public MasterBatchCoordinator(List<String> spoutIds, List<ITridentSpout> spouts) {
        if(spoutIds.isEmpty()) {
            throw new IllegalArgumentException("Must manage at least one spout");
        }
        _managedSpoutIds = spoutIds;
        _spouts = spouts;
    }

    public List<String> getManagedSpoutIds(){
        return _managedSpoutIds;
    }

    @Override
    public void activate() {
        _active = true;
    }

    @Override
    public void deactivate() {
        _active = false;
    }
        
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _throttler = new WindowedTimeThrottler((Number)conf.get(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS), 1);
        for(String spoutId: _managedSpoutIds) {
            _states.add(TransactionalState.newCoordinatorState(conf, spoutId));
        }
        _currTransaction = getStoredCurrTransaction();

        _collector = collector;
        Number active = (Number) conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if(active==null) {
            _maxTransactionActive = 1;
        } else {
            _maxTransactionActive = active.intValue();
        }
        _attemptIds = getStoredCurrAttempts(_currTransaction, _maxTransactionActive);

        
        for(int i=0; i<_spouts.size(); i++) {
            String txId = _managedSpoutIds.get(i);
            _coordinators.add(_spouts.get(i).getCoordinator(txId, conf, context));
        }
    }

    @Override
    public void close() {
        for(TransactionalState state: _states) {
            state.close();
        }
    }

    @Override
    public void nextTuple() {
        sync(Operation.NEXTTUPLE, null);
    }

    @Override
    public void ack(Object msgId) {
        sync(Operation.ACK, (TransactionAttempt) msgId);
    }

    @Override
    public void fail(Object msgId) {
        sync(Operation.FAIL, (TransactionAttempt) msgId);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // in partitioned example, in case an emitter task receives a later transaction than it's emitted so far,
        // when it sees the earlier txid it should know to emit nothing
        declarer.declareStream(BATCH_STREAM_ID, new Fields("tx"));
        declarer.declareStream(COMMIT_STREAM_ID, new Fields("tx"));
        declarer.declareStream(SUCCESS_STREAM_ID, new Fields("tx"));
    }
    
    synchronized private void sync(Operation op, TransactionAttempt attempt) {
        TransactionStatus status;
        long txid;
        
        switch (op) {
            case FAIL:
                // Remove the failed one and the items whose id is higher than the failed one.
                // Then those ones will be retried when nextTuple.
                txid = attempt.getTransactionId();
                status = _activeTx.remove(txid);
                if(status!=null && status.attempt.equals(attempt)) {
                    _activeTx.tailMap(txid).clear();
                }
                break;
                
            case ACK:
                txid = attempt.getTransactionId();
                status = _activeTx.get(txid);
                if(status!=null && attempt.equals(status.attempt)) {
                    if(status.status==AttemptStatus.PROCESSING ) {
                        status.status = AttemptStatus.PROCESSED;
                    } else if(status.status==AttemptStatus.COMMITTING) {
                        status.status = AttemptStatus.COMMITTED;
                    }
                }
                break;
        
            case NEXTTUPLE:
                // note that sometimes the tuples active may be less than max_spout_pending, e.g.
                // max_spout_pending = 3
                // tx 1, 2, 3 active, tx 2 is acked. there won't be a commit for tx 2 (because tx 1 isn't committed yet),
                // and there won't be a batch for tx 4 because there's max_spout_pending tx active
                status = _activeTx.get(_currTransaction);
                if (status!=null) {
                    if(status.status == AttemptStatus.PROCESSED) {
                        status.status = AttemptStatus.COMMITTING;
                        _collector.emit(COMMIT_STREAM_ID, new Values(status.attempt), status.attempt);
                    } else if (status.status == AttemptStatus.COMMITTED) {
                        _activeTx.remove(status.attempt.getTransactionId());
                        _attemptIds.remove(status.attempt.getTransactionId());
                        _collector.emit(SUCCESS_STREAM_ID, new Values(status.attempt));
                        _currTransaction = nextTransactionId(status.attempt.getTransactionId());
                        for(TransactionalState state: _states) {
                            state.setData(CURRENT_TX, _currTransaction);
                        }
                    }
                }

                if(_active) {
                    if(_activeTx.size() < _maxTransactionActive) {
                        Long curr = _currTransaction;
                        for(int i=0; i<_maxTransactionActive; i++) {
                            if(batchDelay()) {
                                break;
                            }
                            
                            if(isReady(curr)) {
                                if(!_activeTx.containsKey(curr)) {
                                    // by using a monotonically increasing attempt id, downstream tasks
                                    // can be memory efficient by clearing out state for old attempts
                                    // as soon as they see a higher attempt id for a transaction
                                    Integer attemptId = _attemptIds.get(curr);
                                    if(attemptId==null) {
                                       attemptId = 0;
                                    } else {
                                       attemptId++;
                                    }
                                    _attemptIds.put(curr, attemptId);
                                    for(TransactionalState state: _states) {
                                        state.setData(CURRENT_ATTEMPTS, _attemptIds);
                                    }
                        
                                    TransactionAttempt currAttempt = new TransactionAttempt(curr, attemptId);
                                    _activeTx.put(curr, new TransactionStatus(currAttempt));
                                    _collector.emit(BATCH_STREAM_ID, new Values(currAttempt), currAttempt);                   
                                    _throttler.markEvent();
                                    break;
                                } 
                            }
                            curr = nextTransactionId(curr);
                        }
                    } else {
                        // Do nothing
                    }
                }
                break;
            
            default:
                LOG.warn("Unknow Operation code=" + op);
                break;
        }
    }
    
    private boolean isReady(long txid) {
        //TODO: make this strategy configurable?... right now it goes if anyone is ready
        for(ITridentSpout.BatchCoordinator coord: _coordinators) {
            if(coord.isReady(txid)) return true;
        }
        return false;
    }
    
    private boolean batchDelay() {
        return _throttler.isThrottled();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        ret.registerSerialization(TransactionAttempt.class);
        return ret;
    }
    
    private static enum AttemptStatus {
        PROCESSING,
        PROCESSED,
        COMMITTING,
        COMMITTED
    }
    
    private static class TransactionStatus {
        TransactionAttempt attempt;
        AttemptStatus status;
        
        public TransactionStatus(TransactionAttempt attempt) {
            this.attempt = attempt;
            this.status = AttemptStatus.PROCESSING;
        }

        @Override
        public String toString() {
            return attempt.toString() + " <" + status.toString() + ">";
        }        
    }
    
    
    private Long nextTransactionId(Long id) {
        return id + 1;
    }  
    
    private Long getStoredCurrTransaction() {
        Long ret = INIT_TXID;
        for(TransactionalState state: _states) {
            Long curr = (Long) state.getData(CURRENT_TX);
            if(curr!=null && curr.compareTo(ret) > 0) {
                ret = curr;
            }
        }
        return ret;
    }
    
    private TreeMap<Long, Integer> getStoredCurrAttempts(long currTransaction, int maxBatches) {
        TreeMap<Long, Integer> ret = new TreeMap<Long, Integer>();
        for(TransactionalState state: _states) {
            Map<Object, Number> attempts = (Map) state.getData(CURRENT_ATTEMPTS);
            if(attempts==null) attempts = new HashMap();
            for(Entry<Object, Number> e: attempts.entrySet()) {
                // this is because json doesn't allow numbers as keys...
                // TODO: replace json with a better form of encoding
                Number txidObj;
                if(e.getKey() instanceof String) {
                    txidObj = Long.parseLong((String) e.getKey());
                } else {
                    txidObj = (Number) e.getKey();
                }
                long txid = ((Number) txidObj).longValue();
                int attemptId = ((Number) e.getValue()).intValue();
                Integer curr = ret.get(txid);
                if(curr==null || attemptId > curr) {
                    ret.put(txid, attemptId);
                }                
            }
        }
        ret.headMap(currTransaction).clear();
        ret.tailMap(currTransaction + maxBatches - 1).clear();
        return ret;
    }
}
