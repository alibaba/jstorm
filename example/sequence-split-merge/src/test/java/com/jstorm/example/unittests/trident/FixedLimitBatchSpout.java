package com.jstorm.example.unittests.trident;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by binyang.dby on 2016/7/9.
 *
 * This is a finite version of the FixedBatchSpout class.
 * Using the BasicLimitBatchSpout, pick at most maxBatchSize content in the outputs and emit as
 * a batch. The content will be circle but has a limit.
 */
public class FixedLimitBatchSpout extends BasicLimitBatchSpout
{
    private List<Object>[] outputs;
    private int index;

    public FixedLimitBatchSpout(long limit, Fields fields, int maxBatchSize, List<Object>... outputs) {
        super(limit, fields, maxBatchSize);
        this.outputs = outputs;
    }

    @Override
    protected List<List<Object>> getBatchContent(int maxBatchSize) {
        List<List<Object>> returnBatch = new ArrayList<List<Object>>();

        if(index >= outputs.length)
            index = 0;

        for(int i = 0; i < maxBatchSize && index < outputs.length; i++, index++)
            returnBatch.add(outputs[index]);

        return returnBatch;
    }
}
