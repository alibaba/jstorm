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
package backtype.storm.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ControlSpoutExecutor implements IRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(ControlSpoutExecutor.class);

    private IControlSpout _spout;

    public ControlSpoutExecutor(IControlSpout spout) {
        _spout = spout;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _spout.open(conf, context, new ControlSpoutOutputCollector(collector));
    }

    @Override
    public void close() {
        _spout.close();

    }

    @Override
    public void activate() {
        _spout.activate();
    }

    @Override
    public void deactivate() {
        _spout.deactivate();

    }

    @Override
    public void nextTuple() {
        _spout.nextTuple();

    }

    @Override
    public void ack(Object msgId) {
        _spout.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        _spout.fail(msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return  _spout.getComponentConfiguration();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }
}