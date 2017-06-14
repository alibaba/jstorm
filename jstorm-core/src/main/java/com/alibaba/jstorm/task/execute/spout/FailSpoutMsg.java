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

import backtype.storm.spout.ISpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.daemon.worker.JStormDebugger;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.comm.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Called after spout receives a failed tuple or times out when sending a tuple
 *
 * @author yannian/Longda
 */
public class FailSpoutMsg implements IAckMsg {
    private static Logger LOG = LoggerFactory.getLogger(FailSpoutMsg.class);
    private Object id;
    private ISpout spout;
    private TupleInfo tupleInfo;
    private TaskBaseMetric taskStats;

    public FailSpoutMsg(Object id, ISpout spout, TupleInfo tupleInfo, TaskBaseMetric taskStats) {
        this.id = id;
        this.spout = spout;
        this.tupleInfo = tupleInfo;
        this.taskStats = taskStats;
    }

    public void run() {
        Object msgId = tupleInfo.getMessageId();

        if (spout instanceof IFailValueSpout) {
            IFailValueSpout enhanceSpout = (IFailValueSpout) spout;
            enhanceSpout.fail(msgId, tupleInfo.getValues());
        } else {
            spout.fail(msgId);
        }

        taskStats.spout_failed_tuple(tupleInfo.getStream());

        if (JStormDebugger.isDebug(id)) {
            LOG.info("Failed message rootId: {}, messageId:{} : {}", id, msgId, tupleInfo.getValues().toString());
        }
    }

}
