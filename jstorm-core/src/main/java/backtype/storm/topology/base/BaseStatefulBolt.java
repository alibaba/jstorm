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
package backtype.storm.topology.base;

import java.util.Map;

import backtype.storm.state.State;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IStatefulBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

public abstract class BaseStatefulBolt<T extends State> implements IStatefulBolt<T> {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // NOOP
    }

    @Override
    public void cleanup() {
        // NOOP
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NOOP
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void preCommit(long txid) {
        // NOOP
    }

    @Override
    public void prePrepare(long txid) {
        // NOOP
    }

    @Override
    public void preRollback() {
        // NOOP
    }
}
