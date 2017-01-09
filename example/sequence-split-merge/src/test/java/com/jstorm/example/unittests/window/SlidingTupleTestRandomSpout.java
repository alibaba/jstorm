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
 * A little bit similar to SlidingWindowTestRandomSpout. But there is still some
 * difference between them.The SlidingWindowTestRandomSpout is used for window
 * which base on the elements count, this class is used for window base on a
 * timestamp field. Since using the real time may cause the result become unpredictable,
 * I use manual generate timestamp instead here. I store a baseTimestamp and add it
 * by 100(us) every time I emit. So it is easy to see if the window works well.
 */
public class SlidingTupleTestRandomSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random;
    private long msgId = 1;
    private int limit;
    private MetricClient metricClient;
    private AsmCounter asmCounter;
    private long baseTimeMillis;

    //these collections are just use for validate the unit test, not the usage of Window Example.
    private List<Integer> sumBoltWindow;
    private List<Integer> sumBoltBuffer;
    private int boltDropCount;

    public SlidingTupleTestRandomSpout(int limit) {
        this.limit = limit;
        //the base timestamp, DO NOT use System.currentTimeMillis because that will
        //cause the bolt window shift and become unpredictable. In that case, the
        //unit test is hard to check if the window works correctly.
        baseTimeMillis =  24 * 60 * 60 * 1000;
        sumBoltWindow = new ArrayList<Integer>();
        sumBoltBuffer = new ArrayList<Integer>();
        for(int i=0; i<SlidingTupleTsTopologyTest.WINDOW_LENGTH_SEC*10; i++)
            sumBoltWindow.add(0);
        int dropSlideWindow = (int) Math.ceil(SlidingTupleTsTopologyTest.WINDOW_LAG_SEC / (SlidingTupleTsTopologyTest.WINDOW_SLIDE_SEC * 1.0f));
        boltDropCount = dropSlideWindow * SlidingTupleTsTopologyTest.WINDOW_SLIDE_SEC * 10;
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
        this.asmCounter = metricClient.registerCounter("SlidingTupleTsTopologyTest.SpoutSum");
    }

    @Override
    public void nextTuple() {
        if(msgId > limit)
        {
            JStormUtils.sleepMs(1000);
            return;
        }

        JStormUtils.sleepMs(50);
        int randomInt = random.nextInt(100);
//        randomInt = (int) msgId;

        sumBoltBuffer.add(randomInt);
        if(sumBoltBuffer.size() == SlidingTupleTsTopologyTest.WINDOW_SLIDE_SEC*10)
        {
            List<Integer> list = new ArrayList<>();
            for(int i=SlidingTupleTsTopologyTest.WINDOW_SLIDE_SEC*10; i<SlidingTupleTsTopologyTest.WINDOW_LENGTH_SEC*10; i++) {
                if(i < sumBoltWindow.size())
                    list.add(sumBoltWindow.get(i));
            }
            list.addAll(sumBoltBuffer);
            sumBoltBuffer.clear();
            sumBoltWindow = list;

            int sum = getSumBoltWindowSum();

            if(msgId <= SlidingTupleTsTopologyTest.SPOUT_LIMIT - boltDropCount)
                asmCounter.update(sum);
        }

        baseTimeMillis += 100;
        long emitTimestamp = baseTimeMillis;
        collector.emit(new Values(randomInt, emitTimestamp, msgId++), msgId);
    }

    private int getSumBoltWindowSum()
    {
        int result = 0;
        for(int i : sumBoltWindow)
            result += i;

        return result;
    }
}
