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

import java.util.Collection;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.task.master.TMHandler;
import com.alibaba.jstorm.task.master.TopologyMaster;
import com.alibaba.jstorm.task.master.TopologyMasterContext;

public class TMUdfHandler implements TMHandler {
    public static class TMUdfMessage {
        public int spoutTaskId;

        public TMUdfMessage(int taskId) {
            this.spoutTaskId = taskId;
        }
    }

    Collection<Integer> boltTasks;
    OutputCollector collector;

    @Override
    public void init(TopologyMasterContext tmContext) {
        TopologyContext context = tmContext.getContext();
        boltTasks = context.getComponentTasks("TMUdfBolt");
        collector = tmContext.getCollector();
    }

    @Override
    public void process(Object event) throws Exception {
        Tuple tuple = (Tuple) event;
        for (int boltTaskId : boltTasks) {
            collector.emitDirectCtrl(boltTaskId, TopologyMaster.USER_DEFINED_STREAM, tuple.getValues());
        }
    }

    @Override
    public void cleanup() {
        
    }
    
}