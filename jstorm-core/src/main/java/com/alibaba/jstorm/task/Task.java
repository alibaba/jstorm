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

import backtype.storm.Config;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import clojure.lang.Atom;
import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.timer.TaskHeartbeatTrigger;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.execute.spout.MultipleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SingleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SpoutExecutors;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Task instance
 *
 * @author yannian/Longda
 */
public class Task implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Task.class);

    private Map<Object, Object> stormConf;

    private TopologyContext topologyContext;
    private TopologyContext userContext;
    private IContext context;

    private TaskTransfer taskTransfer;
    private TaskReceiver taskReceiver;
    private Map<Integer, DisruptorQueue> innerTaskTransfer;
    private Map<Integer, DisruptorQueue> deserializeQueues;
    private Map<Integer, DisruptorQueue> controlQueues;
    private AsyncLoopDefaultKill workHalt;

    private String topologyId;
    private Integer taskId;
    private String componentId;
    private volatile TaskStatus taskStatus;
    private Atom openOrPrepareWasCalled;
    // running time counter
    private UptimeComputer uptime = new UptimeComputer();

    private StormClusterState zkCluster;
    private Object taskObj;
    private TaskBaseMetric taskStats;
    private WorkerData workerData;

    private TaskSendTargets taskSendTargets;
    private TaskReportErrorAndDie reportErrorDie;

    private boolean isTaskBatchTuple;
    private TaskShutdownDameon taskShutdownDameon;
    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    private BaseExecutors baseExecutors;

    @SuppressWarnings("rawtypes")
    public Task(WorkerData workerData, int taskId) throws Exception {
        openOrPrepareWasCalled = new Atom(false);

        this.workerData = workerData;
        this.topologyContext = workerData.getContextMaker().makeTopologyContext(workerData.getSysTopology(), taskId, openOrPrepareWasCalled);
        this.userContext = workerData.getContextMaker().makeTopologyContext(workerData.getRawTopology(), taskId, openOrPrepareWasCalled);
        this.taskId = taskId;
        this.componentId = topologyContext.getThisComponentId();
        this.stormConf = Common.component_conf(workerData.getStormConf(), topologyContext, componentId);

        this.taskStatus = new TaskStatus();

        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.deserializeQueues = workerData.getDeserializeQueues();
        this.controlQueues = workerData.getControlQueues();
        this.topologyId = workerData.getTopologyId();
        this.context = workerData.getContext();
        this.workHalt = workerData.getWorkHalt();
        this.zkCluster = workerData.getZkCluster();
        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();
        // create report error callback,
        // in fact it is storm_cluster.report-task-error
        ITaskReportErr reportError = new TaskReportError(zkCluster, topologyId, taskId);

        // report error and halt worker
        reportErrorDie = new TaskReportErrorAndDie(reportError, workHalt);
        this.taskStats = new TaskBaseMetric(topologyId, componentId, taskId);
        //register auto hook
        List<String> listHooks = Config.getTopologyAutoTaskHooks(stormConf);
        for (String hook : listHooks) {
            ITaskHook iTaskHook = (ITaskHook) Utils.newInstance(hook);
            userContext.addTaskHook(iTaskHook);
        }

        LOG.info("Begin to deserialize taskObj " + componentId + ":" + this.taskId);

        try {
            WorkerClassLoader.switchThreadContext();
            this.taskObj = Common.get_task_object(topologyContext.getRawTopology(), componentId, WorkerClassLoader.getInstance());
            WorkerClassLoader.restoreThreadContext();
        } catch (Exception e) {
            if (reportErrorDie != null) {
                reportErrorDie.report(e);
            } else {
                throw e;
            }
        }
        isTaskBatchTuple = ConfigExtension.isTaskBatchTuple(stormConf);
        LOG.info("Transfer/receive in batch mode :" + isTaskBatchTuple);

        LOG.info("Loading task " + componentId + ":" + this.taskId);
    }

    private TaskSendTargets makeSendTargets() {
        String component = topologyContext.getThisComponentId();

        // get current task's output
        // <Stream_id,<component, Grouping>>
        Map<String, Map<String, MkGrouper>> streamComponentGrouper = Common.outbound_components(topologyContext, workerData);

        return new TaskSendTargets(stormConf, component, streamComponentGrouper, topologyContext, taskStats);
    }

    private void updateSendTargets() {
        if (taskSendTargets != null) {
            Map<String, Map<String, MkGrouper>> streamComponentGrouper = Common.outbound_components(topologyContext, workerData);
            taskSendTargets.updateStreamCompGrouper(streamComponentGrouper);
        } else {
            LOG.error("taskSendTargets is null when trying to update it.");
        }
    }

    public TaskSendTargets echoToSystemBolt() {
        // send "startup" tuple to system bolt
        List<Object> msg = new ArrayList<Object>();
        msg.add("startup");

        // create task receive object
        TaskSendTargets sendTargets = makeSendTargets();

        UnanchoredSend.send(topologyContext, sendTargets, taskTransfer, Common.SYSTEM_STREAM_ID, msg);
        return sendTargets;
    }

    public boolean isSingleThread(Map conf) {
        boolean isOnePending = JStormServerUtils.isOnePending(conf);
        if (isOnePending == true) {
            return true;
        }
        return ConfigExtension.isSpoutSingleThread(conf);
    }

    public BaseExecutors mkExecutor() {
        BaseExecutors baseExecutor = null;

        if (taskObj instanceof IBolt) {
            baseExecutor = new BoltExecutors(this);
        } else if (taskObj instanceof ISpout) {
            if (isSingleThread(stormConf) == true) {
                baseExecutor = new SingleThreadSpoutExecutors(this);
            } else {
                baseExecutor = new MultipleThreadSpoutExecutors(this);
            }
        }

        return baseExecutor;
    }

    /**
     * create executor to receive tuples and run bolt/spout execute function
     */
    private RunnableCallback prepareExecutor() {

        final BaseExecutors baseExecutor = mkExecutor();

        return baseExecutor;
    }

    public TaskReceiver mkTaskReceiver() {
        String taskName = JStormServerUtils.getName(componentId, taskId);
        //if (isTaskBatchTuple)
        //    taskReceiver = new TaskBatchReceiver(this, taskId, stormConf, topologyContext, innerTaskTransfer, taskStatus, taskName);
        //else
        taskReceiver = new TaskReceiver(this, taskId, stormConf, topologyContext, innerTaskTransfer, taskStatus, taskName);
        deserializeQueues.put(taskId, taskReceiver.getDeserializeQueue());

        return taskReceiver;
    }

    public TaskShutdownDameon execute() throws Exception {

        taskSendTargets = echoToSystemBolt();

        // create thread to get tuple from zeroMQ,
        // and pass the tuple to bolt/spout
        taskTransfer = mkTaskSending(workerData);
        RunnableCallback baseExecutor = prepareExecutor();
        //set baseExecutors for update
        setBaseExecutors((BaseExecutors)baseExecutor);

        AsyncLoopThread executor_threads = new AsyncLoopThread(baseExecutor, false, Thread.MAX_PRIORITY, true);
        taskReceiver = mkTaskReceiver();

        List<AsyncLoopThread> allThreads = new ArrayList<AsyncLoopThread>();
        allThreads.add(executor_threads);

        LOG.info("Finished loading task " + componentId + ":" + taskId);

        taskShutdownDameon = getShutdown(allThreads, baseExecutor);
        return taskShutdownDameon;
    }

    private TaskTransfer mkTaskSending(WorkerData workerData) {
        // sending tuple's serializer
        KryoTupleSerializer serializer = new KryoTupleSerializer(workerData.getStormConf(), topologyContext.getRawTopology());
        String taskName = JStormServerUtils.getName(componentId, taskId);
        // Task sending all tuples through this Object
        TaskTransfer taskTransfer;
        taskTransfer = new TaskTransfer(this, taskName, serializer, taskStatus, workerData, topologyContext);
        return taskTransfer;
    }

    public TaskShutdownDameon getShutdown(List<AsyncLoopThread> allThreads, RunnableCallback baseExecutor) {
        AsyncLoopThread ackerThread = null;
        if (baseExecutor instanceof SpoutExecutors) {
            ackerThread = ((SpoutExecutors) baseExecutor).getAckerRunnableThread();

            if (ackerThread != null) {
                allThreads.add(ackerThread);
            }
        }
        List<AsyncLoopThread> recvThreads = taskReceiver.getDeserializeThread();
        for (AsyncLoopThread recvThread : recvThreads) {
            allThreads.add(recvThread);
        }

        List<AsyncLoopThread> serializeThreads = taskTransfer.getSerializeThreads();
        allThreads.addAll(serializeThreads);
        TaskHeartbeatTrigger taskHeartbeatTrigger = ((BaseExecutors) baseExecutor).getTaskHbTrigger();

        TaskShutdownDameon shutdown = new TaskShutdownDameon(taskStatus, topologyId, taskId, allThreads, zkCluster, taskObj, this, taskHeartbeatTrigger);

        return shutdown;
    }

    public TaskShutdownDameon getTaskShutdownDameon() {
        return taskShutdownDameon;
    }

    public void run() {
        try {
            taskShutdownDameon = this.execute();
        } catch (Throwable e) {
            LOG.error("init task take error", e);
            if (reportErrorDie != null) {
                reportErrorDie.report(e);
            } else {
                throw new RuntimeException(e);
            }

        }
    }

    public static TaskShutdownDameon mk_task(WorkerData workerData, int taskId) throws Exception {
        Task t = new Task(workerData, taskId);
        return t.execute();
    }

    /**
     * Update the data which can be changed dynamically e.g. when scale-out of a task parallelism
     */
    public void updateTaskData() {
        // Only update the local task list of topologyContext here. Because
        // other
        // relative parts in context shall be updated while the updating of
        // WorkerData (Task2Component and Component2Task map)
        List<Integer> localTasks = JStormUtils.mk_list(workerData.getTaskids());
        topologyContext.setThisWorkerTasks(localTasks);
        userContext.setThisWorkerTasks(localTasks);

        // Update the TaskSendTargets
        updateSendTargets();
    }

    public long getWorkerAssignmentTs() {
        return workerData.getAssignmentTs();
    }

    public AssignmentType getWorkerAssignmentType() {
        return workerData.getAssignmentType();
    }

    public void unregisterDeserializeQueue() {
        deserializeQueues.remove(taskId);
    }

    public String getComponentId() {
        return componentId;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public DisruptorQueue getExecuteQueue() {
        return innerTaskTransfer.get(taskId);
    }

    public DisruptorQueue getDeserializeQueue() {
        return deserializeQueues.get(taskId);
    }

    public Map<Object, Object> getStormConf() {
        return stormConf;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    public TopologyContext getUserContext() {
        return userContext;
    }

    public TaskTransfer getTaskTransfer() {
        return taskTransfer;
    }

    public TaskReceiver getTaskReceiver() {
        return taskReceiver;
    }

    public Map<Integer, DisruptorQueue> getInnerTaskTransfer() {
        return innerTaskTransfer;
    }

    public Map<Integer, DisruptorQueue> getDeserializeQueues() {
        return deserializeQueues;
    }

    public Map<Integer, DisruptorQueue> getControlQueues() {
        return controlQueues;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public StormClusterState getZkCluster() {
        return zkCluster;
    }

    public Object getTaskObj() {
        return taskObj;
    }

    public TaskBaseMetric getTaskStats() {
        return taskStats;
    }

    public WorkerData getWorkerData() {
        return workerData;
    }

    public TaskSendTargets getTaskSendTargets() {
        return taskSendTargets;
    }

    public TaskReportErrorAndDie getReportErrorDie() {
        return reportErrorDie;
    }

    public boolean isTaskBatchTuple() {
        return isTaskBatchTuple;
    }

    public ConcurrentHashMap<WorkerSlot, IConnection> getNodeportSocket() {
        return nodeportSocket;
    }

    public ConcurrentHashMap<Integer, WorkerSlot> getTaskNodeport() {
        return taskNodeport;
    }

    public BaseExecutors getBaseExecutors() {
        return baseExecutors;
    }

    public void setBaseExecutors(BaseExecutors baseExecutors) {
        this.baseExecutors = baseExecutors;
    }

}
