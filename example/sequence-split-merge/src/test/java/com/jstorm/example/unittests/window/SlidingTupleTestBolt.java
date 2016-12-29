package com.jstorm.example.unittests.window;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.windowing.TupleWindow;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;

import java.util.List;
import java.util.Map;

/**
 * Created by binyang.dby on 2016/7/21.
 */
public class SlidingTupleTestBolt extends BaseWindowedBolt {

    private MetricClient metricClient;
    private AsmCounter asmCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.metricClient = new MetricClient(context);
        this.asmCounter = metricClient.registerCounter("SlidingTupleTsTopologyTest.BoltSum");
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        List<Tuple> tuplesInWindow = inputWindow.get();
        int sum = 0;
        for(Tuple tuple : tuplesInWindow)
            sum += tuple.getInteger(0);

        asmCounter.update(sum);
    }
}
