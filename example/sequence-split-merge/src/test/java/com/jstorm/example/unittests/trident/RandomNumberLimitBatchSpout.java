package com.jstorm.example.unittests.trident;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by binyang.dby on 2016/7/9.
 *
 * This is a finite version of the RandomNumberGeneratorSpout class.
 */
public class RandomNumberLimitBatchSpout extends BasicLimitBatchSpout {
    private int maxNumber;
    private int fieldSize;

    public RandomNumberLimitBatchSpout(long limit, Fields fields, int maxBatchSize, int maxNumber) {
        super(limit, fields, maxBatchSize);
        this.maxNumber = maxNumber;
        this.fieldSize = fields.size();
    }

    @Override
    protected List<List<Object>> getBatchContent(int maxBatchSize) {
        List<List<Object>> returnBatch = new ArrayList<List<Object>>();
        for(int i=0; i<maxBatchSize; i++)
        {
            List<Object> numbers = new ArrayList<Object>();
            for(int j=0; j<fieldSize; j++)
                numbers.add(ThreadLocalRandom.current().nextInt(0, maxNumber + 1));

            returnBatch.add(numbers);
        }

        return returnBatch;
    }
}
