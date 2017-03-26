package com.jstorm.example.unittests.window;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by binyang.dby on 2016/7/21.
 *
 * randomly generate an Integer sequence with a given limit.
 * Because this class is only used for unit test SlidingWindowTopologyTest,
 * so I add some code to calculate what value should the final bolt generate.
 * I compare the value got in both spout and bolt, and let it pass if the 2
 * values are the same.
 */
public class SlidingWindowTestRandomSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private Random random;
    private long msgId = 1;
    private int limit;
    private MetricClient metricClient;
    private AsmCounter asmCounter;

    //these collections are just use for validate the unit test, not the usage of Window Example.
    private List<Integer> sumBoltWindow;
    private List<Integer> sumBoltBuffer;
    private List<Integer> avgBoltWindow;

    public SlidingWindowTestRandomSpout(int limit) {
        sumBoltWindow = new ArrayList<Integer>();
        sumBoltBuffer = new ArrayList<Integer>();
        avgBoltWindow = new ArrayList<Integer>();
        this.limit = limit;

        for(int i=0; i<SlidingWindowTopologyTest.SUM_BOLT_WINDOW_LENGTH; i++)
            sumBoltWindow.add(0);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value", "ts", "msgid"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        this.metricClient = new MetricClient(context);
        this.asmCounter = metricClient.registerCounter("SlidingWindowTopologyTest.SpoutAvgSum");
    }

    @Override
    public void nextTuple() {
        if(msgId > limit)
        {
            JStormUtils.sleepMs(1000);
            return;
        }

        JStormUtils.sleepMs(5);
        int randomInt = random.nextInt(100);

        //the following codes are used to calculate what the average value should be and add it up.
        //the result will be used to validate the unit test.
        sumBoltBuffer.add(randomInt);
        if(sumBoltBuffer.size() == SlidingWindowTopologyTest.SUM_BOLT_WINDOW_SLIDE)
        {
            List<Integer> list = new ArrayList<>();
            for(int i=SlidingWindowTopologyTest.SUM_BOLT_WINDOW_SLIDE; i<SlidingWindowTopologyTest.SUM_BOLT_WINDOW_LENGTH; i++) {
                if(i < sumBoltWindow.size())
                    list.add(sumBoltWindow.get(i));
            }
            list.addAll(sumBoltBuffer);
            sumBoltBuffer.clear();
            sumBoltWindow = list;

            //elements reach SUM_BOLT_WINDOW_SLIDE
            //the SumBolt will sum up the elements in the window
            int sum = getSumBoltWindowSum();

            //the SumBolt send the sum value to AvgBolt
            avgBoltWindow.add(sum);

            if(avgBoltWindow.size() == SlidingWindowTopologyTest.AVG_BOLT_WINDOW_LENGTH)
            {
                //elements reach AVG_BOLT_WINDOW_LENGTH
                //calculate the average value and clear the window
                int avg = getAvgBoltWindowAvg();
                asmCounter.update(avg);
                avgBoltWindow.clear();
            }
        }

        collector.emit(new Values(randomInt, System.currentTimeMillis() - (24 * 60 * 60 * 1000), msgId++), msgId);
    }

    private int getSumBoltWindowSum()
    {
        int result = 0;
        for(int i : sumBoltWindow)
            result += i;

        return result;
    }

    private int getAvgBoltWindowAvg()
    {
        int sum = 0;
        for(int i : avgBoltWindow)
            sum += i;

        return sum/avgBoltWindow.size();
    }
}
