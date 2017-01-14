package com.alipay.dw.jstorm.example.deploy;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DeployBolt extends BaseRichBolt {
    private final static Logger LOG = LoggerFactory.getLogger(DeployBolt.class);
    private OutputCollector _collector;
    private Map<String, List<Integer>> _sourceTasks;
    private Map<String, List<Integer>> _targetTasks;
    private volatile Integer _expectExcevieNum = 0;
    private Integer timeout;
    private TopologyContext context;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _sourceTasks = context.getThisSourceComponentTasks();
        _targetTasks = context.getThisTargetComponentTasks();
        for (List<Integer> tasks : _sourceTasks.values()) {
            _expectExcevieNum += tasks.size();
        }
        this.context = context;
        timeout = ConfigExtension.getTaskCleanupTimeoutSec(conf) / 2;

        LOG.info("this component's sourceTasks is {}, and the target tasks is {}", _sourceTasks, _targetTasks);
    }

    public void execute(Tuple tuple) {
        if (tuple.getValues().size() == 0) {
            _expectExcevieNum--;
        } else {
            JStormUtils.sleepMs(1);
            _collector.emit(tuple, new Values(tuple.getValues()));
            _collector.ack(tuple);
        }
    }

    public void cleanup() {
        if (JStormUtils.isKilledStatus(context)){
            long start = System.currentTimeMillis();

            while (_expectExcevieNum != 0) {
                if (_expectExcevieNum < 0) {
                    LOG.warn("_expectExcevieNum can't less than zero");
                    break;
                }
                JStormUtils.sleepMs(1);
                if (System.currentTimeMillis() - start > timeout * 1000) {
                    LOG.warn("timeout, this component still not receive {} finish stream", _expectExcevieNum);
                    break;
                }
            }
            JStormUtils.sleepMs(10);
            int sendNum = 0;
            for (List<Integer> tasks : _targetTasks.values()) {
                for (Integer task : tasks) {
                    sendNum++;
                    _collector.emitDirect(task, new Values());
                }
            }
            _collector.flush();
            LOG.info("this component-{} already sent {} finish messages", context.getThisTaskId(), sendNum);
            JStormUtils.sleepMs(100);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

}