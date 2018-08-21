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
package com.alipay.dw.jstorm.example.tm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.task.master.TopologyMaster;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alipay.dw.jstorm.example.tm.TMUdfHandler.TMUdfMessage;;

public class TMUdfSpout implements IRichSpout {
    private int topologyMasterId;
    private int spoutTaskId;
    SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.topologyMasterId = context.getTopologyMasterId();
        this.spoutTaskId = context.getThisTaskId();
        this.collector = collector;
    }

    @Override
    public void close() {
        
    }

    @Override
    public void activate() {
        
    }

    @Override
    public void deactivate() {
        
    }

    @Override
    public void nextTuple() {
        TMUdfMessage message = new TMUdfMessage(spoutTaskId);
        collector.emitDirectCtrl(topologyMasterId, TopologyMaster.USER_DEFINED_STREAM, new Values(message));
        JStormUtils.sleepMs(10000);
    }

    @Override
    public void ack(Object msgId) {
        
    }

    @Override
    public void fail(Object msgId) {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
}