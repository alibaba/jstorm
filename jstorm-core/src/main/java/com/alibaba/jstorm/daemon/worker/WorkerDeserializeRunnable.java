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
package com.alibaba.jstorm.daemon.worker;

import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class WorkerDeserializeRunnable extends RunnableCallback {

    private static Logger LOG = LoggerFactory.getLogger(WorkerDeserializeRunnable.class);
    private volatile List<TaskShutdownDameon> shutdownTasks;
    private int threadIndex;
    private int startRunTaskIndex;
    private KryoTupleDeserializer deserializer;

    public WorkerDeserializeRunnable(List<TaskShutdownDameon> shutdownTasks,
                                     Map stormConf,
                                     GeneralTopologyContext topologyContext,
                                     int startRunTaskIndex,
                                     int threadIndex) {
        this.shutdownTasks = shutdownTasks;
        this.threadIndex = threadIndex;
        this.startRunTaskIndex = startRunTaskIndex;
        this.deserializer = new KryoTupleDeserializer(stormConf, topologyContext, topologyContext.getRawTopology());
    }

    @Override
    public String getThreadName() {
        return "worker-deserializer-" + threadIndex;
    }

    @Override
    public void preRun() {
        WorkerClassLoader.switchThreadContext();
    }

    @Override
    public void postRun() {
        WorkerClassLoader.restoreThreadContext();
    }

    @Override
    public void run() {
        LOG.info("Successfully started " + getThreadName());
        while (!AsyncLoopRunnable.getShutdown().get()) {
            int loopCount = shutdownTasks.size();
            //note: avoid to cpu idle
            boolean isIdling = true;
            for (int i = 0; i < loopCount; i++) {
                try {
                    if (startRunTaskIndex >= shutdownTasks.size())
                        startRunTaskIndex = 0;
                    TaskShutdownDameon taskShutdownDameon = shutdownTasks.get(startRunTaskIndex);
                    boolean ret = taskShutdownDameon.getTask().getTaskReceiver().deserializer(deserializer, false);
                    if (!ret) {
                        isIdling = false;
                    }
                    startRunTaskIndex++;
                } catch (IndexOutOfBoundsException ignored) {
                }
            }
            if (isIdling) {
                try {
                    if (startRunTaskIndex >= shutdownTasks.size())
                        startRunTaskIndex = 0;
                    TaskShutdownDameon taskShutdownDameon = shutdownTasks.get(startRunTaskIndex);
                    taskShutdownDameon.getTask().getTaskReceiver().deserializer(deserializer, true);
                    startRunTaskIndex++;
                } catch (IndexOutOfBoundsException e) {
                    //inore
                }
            }
        }
    }

    public Object getResult() {
        LOG.info("Begin to shutdown " + getThreadName());
        return -1;
    }
}
