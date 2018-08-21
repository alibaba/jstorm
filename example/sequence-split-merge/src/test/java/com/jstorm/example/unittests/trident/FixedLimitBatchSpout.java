/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jstorm.example.unittests.trident;

import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * @author binyang.dby on 2016/7/9.
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
