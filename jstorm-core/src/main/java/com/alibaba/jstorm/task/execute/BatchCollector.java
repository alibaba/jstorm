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
package com.alibaba.jstorm.task.execute;

import backtype.storm.task.ICollectorCallback;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchFlushTrigger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BatchCollector {
    private static Logger LOG = LoggerFactory.getLogger(BatchCollector.class);

    protected Integer taskId;
    protected String componentId;
    protected Map conf;

    protected final Map<String, List<MsgInfo>> streamToBatches = new HashMap<>();
    protected final Map<String, List<MsgInfo>> directBatches = new HashMap<>();

    protected int batchSize;

    public BatchCollector(Integer taskId, String componentId, Map conf) {
        this.taskId = taskId;
        this.componentId = componentId;
        this.conf = conf;
        batchSize = ConfigExtension.getTaskMsgBatchSize(conf);

        int flushTime = ConfigExtension.getTaskMsgFlushInervalMs(conf);
        TaskBatchFlushTrigger batchFlushTrigger = new TaskBatchFlushTrigger(flushTime, componentId + "-" + taskId, this);
        //batchFlushTrigger.register(TimeUnit.MILLISECONDS);
        batchFlushTrigger.start();

        LOG.info("BatchCollector: batchSize=" + batchSize + ", flushTime=" + flushTime);
    }

    public abstract void pushAndSend(String streamId, List<Object> tuple, Integer outTaskId,
                                     Collection<Tuple> anchors, Object messageId, Long rootId,
                                     ICollectorCallback callback);

    public abstract void flush();

    public int getConfigBatchSize() {
        return batchSize;
    }
}