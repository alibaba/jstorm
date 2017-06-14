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
package com.alipay.dw.jstorm.example.sequence.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.starter.utils.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.SequenceTopologyDef;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import com.alipay.dw.jstorm.example.sequence.bean.TradeCustomer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeRecord implements IRichBolt {
    private static final long serialVersionUID = -5984311734042330577L;
    
    public static Logger LOG = LoggerFactory.getLogger(MergeRecord.class);
    
    private Map<Long, Tuple> tradeMap    = new HashMap<Long, Tuple>();
    private Map<Long, Tuple> customerMap = new HashMap<Long, Tuple>();
    
    private TpsCounter      tpsCounter;
    private OutputCollector collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());
        
        LOG.info("Finished preparation");
    }
    
    private AtomicLong tradeSum    = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(0);
    
    @Override
    public void execute(Tuple input) {
        
        tpsCounter.count();
        
        Long tupleId = input.getLong(0);
        Pair pair = (Pair) input.getValue(1);
        
        Pair trade = null;
        Pair customer = null;
        
        Tuple tradeTuple = null;
        Tuple customerTuple = null;
        
        if (input.getSourceComponent().equals(SequenceTopologyDef.CUSTOMER_BOLT_NAME)) {
            customer = pair;
            customerTuple = input;
            
            tradeTuple = tradeMap.remove(tupleId);
            if (tradeTuple == null) {
                customerMap.put(tupleId, input);
                return;
            }
            
            trade = (Pair) tradeTuple.getValue(1);
            
        } else if (input.getSourceComponent().equals(SequenceTopologyDef.TRADE_BOLT_NAME)) {
            trade = pair;
            tradeTuple = input;
            
            customerTuple = customerMap.remove(tupleId);
            if (customerTuple == null) {
                tradeMap.put(tupleId, input);
                return;
            }
            
            customer = (Pair) customerTuple.getValue(1);
        } else {
            LOG.info("Unknow source component: " + input.getSourceComponent());
            collector.fail(input);
            return;
        }
        
        tradeSum.addAndGet(trade.getValue());
        customerSum.addAndGet(customer.getValue());
        
        collector.ack(tradeTuple);
        collector.ack(customerTuple);
        
        TradeCustomer tradeCustomer = new TradeCustomer();
        tradeCustomer.setTrade(trade);
        tradeCustomer.setCustomer(customer);
        collector.emit(new Values(tupleId, tradeCustomer));
    }
    
    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
        LOG.info("Finish cleanup");
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "RECORD"));
    }
    
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
