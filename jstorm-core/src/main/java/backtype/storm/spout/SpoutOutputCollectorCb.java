package backtype.storm.spout;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.topology.IControlOutputCollector;
import backtype.storm.topology.IControlSpoutOutputCollector;
import backtype.storm.tuple.Tuple;

import java.util.Collection;
import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class SpoutOutputCollectorCb implements ISpoutOutputCollector{
    protected ISpoutOutputCollector delegate;

    public SpoutOutputCollectorCb() {

    }

    public SpoutOutputCollectorCb(ISpoutOutputCollector delegate) {
        this.delegate = delegate;
    }

    public abstract List<Integer> emit(String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback);

    public abstract void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId, ICollectorCallback callback);

    public void flush() {

    }
}
