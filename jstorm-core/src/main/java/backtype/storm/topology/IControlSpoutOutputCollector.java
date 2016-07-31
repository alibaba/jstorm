package backtype.storm.topology;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface IControlSpoutOutputCollector {
    List<Integer> emitCtrl(String streamId, List<Object> tuple, Object messageId);

    void emitDirectCtrl(int taskId, String streamId, List<Object> tuple, Object messageId);
}