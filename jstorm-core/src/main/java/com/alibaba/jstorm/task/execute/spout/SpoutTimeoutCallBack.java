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
package com.alibaba.jstorm.task.execute.spout;

import backtype.storm.utils.DisruptorQueue;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.utils.ExpiredCallback;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutTimeoutCallBack<K, V> implements ExpiredCallback<K, V> {
    private static Logger LOG = LoggerFactory.getLogger(SpoutTimeoutCallBack.class);

    private DisruptorQueue disruptorEventQueue;
    private backtype.storm.spout.ISpout spout;
    private Map stormConf;
    private TaskBaseMetric taskStats;

    public SpoutTimeoutCallBack(DisruptorQueue disruptorEventQueue, backtype.storm.spout.ISpout _spout,
                                Map _storm_conf, TaskBaseMetric stat) {
        this.stormConf = _storm_conf;
        this.disruptorEventQueue = disruptorEventQueue;
        this.spout = _spout;
        this.taskStats = stat;
    }

    /**
     * pending.put(root_id, JStormUtils.mk_list(message_id, TupleInfo, ms));
     */
    @Override
    public void expire(K key, V val) {
        if (val == null) {
            return;
        }
        try {
            TupleInfo tupleInfo = (TupleInfo) val;
            FailSpoutMsg fail = new FailSpoutMsg(key, spout, tupleInfo, taskStats);

            disruptorEventQueue.publish(fail);
        } catch (Exception e) {
            LOG.error("expire error", e);
        }
    }
}
