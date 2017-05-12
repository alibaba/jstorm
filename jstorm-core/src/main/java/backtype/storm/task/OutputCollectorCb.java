package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class OutputCollectorCb implements IOutputCollector {
    protected IOutputCollector delegate;

    public OutputCollectorCb() {
    }

    public OutputCollectorCb(IOutputCollector delegate) {
        this.delegate = delegate;
    }

    public abstract List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback);

    public abstract void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, ICollectorCallback callback);

    public abstract List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple);

    public abstract void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);

    public void flush() {

    }

    public void setBatchId(long batchId) {

    }
}
