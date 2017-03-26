package backtype.storm.topology;

import backtype.storm.tuple.Tuple;

import java.util.Collection;
import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface IControlOutputCollector {
    /**
     * emit control message for bolt
     */
    List<Integer> emitCtrl(String streamId, Collection<Tuple> anchors, List<Object> tuple);

    void emitDirectCtrl(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);

}