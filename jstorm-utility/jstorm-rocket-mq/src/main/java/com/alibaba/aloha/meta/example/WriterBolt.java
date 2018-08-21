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
package com.alibaba.aloha.meta.example;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.aloha.meta.MetaTuple;


public class WriterBolt implements IRichBolt {

    private static final long serialVersionUID = 2495121976857546346L;
    
    private static final Logger LOG              = Logger.getLogger(WriterBolt.class);

    protected OutputCollector      collector;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }
    
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        MetaTuple metaTuple = (MetaTuple)tuple.getValue(0);
        
        try {
            LOG.info("Messages:" + metaTuple);
            
        } catch (Exception e) {
            collector.fail(tuple);
            return ;
            //throw new FailedException(e);
        }
        
        collector.ack(tuple);
    }
    
    public void cleanup() {
        // TODO Auto-generated method stub
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
