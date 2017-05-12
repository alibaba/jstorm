package backtype.storm.spout;

import backtype.storm.task.ICollectorCallback;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class SpoutOutputCollectorCb implements ISpoutOutputCollector {
    protected ISpoutOutputCollector delegate;

    public SpoutOutputCollectorCb() {
    }

    public SpoutOutputCollectorCb(ISpoutOutputCollector delegate) {
        this.delegate = delegate;
    }

    public abstract List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback);

    public abstract void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback);

    public abstract void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId);

    public abstract List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId);

    public void emitBarrier() {
        
    }


    public void flush() {
        
    }

    public void setBatchId(long batchId) {
        
    }
}
