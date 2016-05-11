package backtype.storm.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ControlSpoutExecutor implements IRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(ControlSpoutExecutor.class);

    private IControlSpout _spout;

    public ControlSpoutExecutor(IControlSpout spout) {
        _spout = spout;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _spout.open(conf, context, new ControlSpoutOutputCollector(collector));
    }

    @Override
    public void close() {
        _spout.close();

    }

    @Override
    public void activate() {
        _spout.activate();
    }

    @Override
    public void deactivate() {
        _spout.deactivate();

    }

    @Override
    public void nextTuple() {
        _spout.nextTuple();

    }

    @Override
    public void ack(Object msgId) {
        _spout.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        _spout.fail(msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return  _spout.getComponentConfiguration();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }
}