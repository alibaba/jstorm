package backtype.storm.topology;

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface IControlSpout extends IComponent {

    void open(Map conf, TopologyContext context, ControlSpoutOutputCollector collector);

    void close();

    void activate();

    void deactivate();

    void nextTuple();

    void ack(Object msgId);

    void fail(Object msgId);
}

