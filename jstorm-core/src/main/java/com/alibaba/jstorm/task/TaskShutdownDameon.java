/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IDynamicComponent;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.ShutdownableDameon;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.utils.JStormUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * shutdown one task
 *
 * @author yannian/Longda
 */
public class TaskShutdownDameon implements ShutdownableDameon {
    private static Logger LOG = LoggerFactory.getLogger(TaskShutdownDameon.class);

    private Task task;
    private TaskStatus taskStatus;
    private String topologyId;
    private Integer taskId;
    private List<AsyncLoopThread> allThreads;
    private StormClusterState zkCluster;
    private Object taskObj;
    private AtomicBoolean isClosing = new AtomicBoolean(false);
    private TaskHeartbeatTrigger taskHeartbeatTrigger;

    public TaskShutdownDameon(TaskStatus taskStatus, String topologyId, Integer taskId,
                              List<AsyncLoopThread> allThreads, StormClusterState zkCluster,
                              Object taskObj, Task task, TaskHeartbeatTrigger taskHeartbeatTrigger) {
        this.taskStatus = taskStatus;
        this.topologyId = topologyId;
        this.taskId = taskId;
        this.allThreads = allThreads;
        this.zkCluster = zkCluster;
        this.taskObj = taskObj;
        this.task = task;
        this.taskHeartbeatTrigger = taskHeartbeatTrigger;
    }

    @Override
    public void shutdown() {
        if (isClosing.compareAndSet(false, true)) {
            LOG.info("Begin to shut down task " + topologyId + ":" + taskId);

            TopologyContext userContext = task.getUserContext();
            for (ITaskHook iTaskHook : userContext.getHooks())
                iTaskHook.cleanup();
            closeComponent(taskObj);
            taskHeartbeatTrigger.updateExecutorStatus(TaskStatus.SHUTDOWN);

            // wait 1 sec for executor thread to shutdown to make sure to send shutdown info to TM
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }

            // all thread will check the taskStatus
            // once it has been set to SHUTDOWN, it will quit
            taskStatus.setStatus(TaskStatus.SHUTDOWN);

            for (AsyncLoopThread thr : allThreads) {
                LOG.info("Begin to shutdown " + thr.getThread().getName());
                thr.cleanup();
                JStormUtils.sleepMs(10);
                thr.interrupt();
                // try {
                // //thr.join();
                // thr.getThread().stop(new RuntimeException());
                // } catch (Throwable e) {
                // }
                LOG.info("Successfully shutdown " + thr.getThread().getName());
            }

            taskHeartbeatTrigger.unregister();
            LOG.info("Successfully shutdown task heartbeat trigger for task:{}", taskId);

            try {
                zkCluster.disconnect();
            } catch (Exception e) {
                LOG.error("Failed to disconnect zk for task-" + taskId);
            }

            LOG.info("Successfully shutdown task " + topologyId + ":" + taskId);
        }
    }

    public void join() throws InterruptedException {
        for (AsyncLoopThread t : allThreads) {
            t.join();
        }
    }

    private void closeComponent(Object _task_obj) {
        if (_task_obj instanceof IBolt) {
            ((IBolt) _task_obj).cleanup();
        }

        if (_task_obj instanceof ISpout) {
            ((ISpout) _task_obj).close();
        }
    }

    @Override
    public boolean waiting() {
        return taskStatus.isRun();
    }

    public void deactive() {
        if (taskObj instanceof ISpout) {
            taskStatus.setStatus(TaskStatus.PAUSE);
            WorkerClassLoader.switchThreadContext();

            try {
                ((ISpout) taskObj).deactivate();
            } finally {
                WorkerClassLoader.restoreThreadContext();
            }
        } else {
            taskStatus.setStatus(TaskStatus.PAUSE);
        }
    }

    public void active() {
        if (taskObj instanceof ISpout) {
            taskStatus.setStatus(TaskStatus.RUN);
            WorkerClassLoader.switchThreadContext();
            try {
                ((ISpout) taskObj).activate();
            } finally {
                WorkerClassLoader.restoreThreadContext();
            }
        } else {
            taskStatus.setStatus(TaskStatus.RUN);
        }
    }

    public void update(Map conf) {
        if (taskObj instanceof IDynamicComponent) {
            ((IDynamicComponent) taskObj).update(conf);
        } else {
            if (task.getBaseExecutors() != null) {
                task.getBaseExecutors().update(conf);
            }
        }
    }

    @Override
    public void run() {
        shutdown();
    }

    public int getTaskId() {
        return this.taskId;
    }

    public Task getTask() {
        return this.task;
    }

}
