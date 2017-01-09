package com.alipay.dw.jstorm.example.deploy;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DeploySpoult extends BaseRichSpout {

    private final static Logger LOG = LoggerFactory.getLogger(DeploySpoult.class);
    private int _sizeInBytes;
    private long _messageCount;
    private SpoutOutputCollector _collector;
    private String[] _messages = null;
    private boolean _ackEnabled;
    private Random _rand = null;

    private Map<String, List<Integer>> _sourceTasks;
    private Map<String, List<Integer>> _targetTasks;
    private TopologyContext context;


    public DeploySpoult(int sizeInBytes, boolean ackEnabled) {
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
        }
        _sizeInBytes = sizeInBytes;
        _messageCount = 0;
        _ackEnabled = ackEnabled;
    }

    public boolean isDistributed() {
        return true;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _rand = new Random();
        this.context = context;
        _collector = collector;
        final int differentMessages = 100;
        _messages = new String[differentMessages];
        for (int i = 0; i < differentMessages; i++) {
            StringBuilder sb = new StringBuilder(_sizeInBytes);
            //Even though java encodes strings in UCS2, the serialized version sent by the tuples
            // is UTF8, so it should be a single byte
            for (int j = 0; j < _sizeInBytes; j++) {
                sb.append(_rand.nextInt(9));
            }
            _messages[i] = sb.toString();
        }
        _sourceTasks = context.getThisSourceComponentTasks();
        _targetTasks = context.getThisTargetComponentTasks();
        LOG.info("this component's sourceTasks is {}, and the target tasks is {}", _sourceTasks, _targetTasks);
    }

    @Override
    public void close() {

        if (JStormUtils.isKilledStatus(context)){
            JStormUtils.sleepMs(10);
            int sendNum = 0;
            for (List<Integer> tasks : _targetTasks.values()) {
                for (Integer task : tasks) {
                    sendNum++;
                    _collector.emitDirect(task, new Values());
                }
            }
            _collector.flush();
            LOG.info("this component already sent {} finish messages", sendNum);
            JStormUtils.sleepMs(100);
        }
    }


    public void nextTuple() {
        JStormUtils.sleepMs(1);
        final String message = _messages[_rand.nextInt(_messages.length)];
        if (_ackEnabled) {
            _collector.emit(new Values(message), _messageCount);
        } else {
            _collector.emit(new Values(message));
        }
        _messageCount++;
    }


    @Override
    public void ack(Object msgId) {
        //Empty
    }

    @Override
    public void fail(Object msgId) {
        //Empty
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

}