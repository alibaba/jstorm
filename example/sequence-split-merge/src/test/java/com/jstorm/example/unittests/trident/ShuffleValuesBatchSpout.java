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

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.*;

/**
 * @author binyang.dby on 2016/7/22.
 */
public class ShuffleValuesBatchSpout implements IBatchSpout {
    private Fields fields;
    private List<Values> contentA;
    private List<Values> contentB;
    private List<Integer> shuffleIndexes;
    private HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

    public ShuffleValuesBatchSpout(Fields fields, List<Values> contentA, List<Values> contentB) {
        this.fields = fields;
        this.contentA = contentA;
        this.contentB = contentB;
        this.shuffleIndexes = new ArrayList<Integer>();
        if(contentA.size() != contentB.size())
            throw new IllegalArgumentException("2 contents should have the same length!");

        int size = contentA.size();
        for(int i=0; i<size; i++)
            shuffleIndexes.add(i);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        System.out.println("emit " + batchId);
        List<List<Object>> batch;
        if(batches.containsKey(batchId)) {
            batch = batches.get(batchId);
        }
        else {
            batch = getBatchContent();
            this.batches.put(batchId, batch);
        }

        for(List<Object> tuple : batch) {
            tridentCollector.emit(tuple);
        }
    }

    private List<List<Object>> getBatchContent() {
        List<List<Object>> batchContent = new ArrayList<List<Object>>();
        Collections.shuffle(shuffleIndexes);

        for(Integer i : shuffleIndexes)
            batchContent.add(new ArrayList<Object>(contentA.get(i)));

        Collections.shuffle(shuffleIndexes);

        for(int i=0; i< shuffleIndexes.size(); i++) {
            List<Object> values = batchContent.get(i);
            values.addAll(contentB.get(shuffleIndexes.get(i)));
        }

        return batchContent;
    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
}
