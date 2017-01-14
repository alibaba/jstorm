package com.alibaba.jstorm.daemon.worker;

import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class WorkerSerializeRunnable extends RunnableCallback {

    private static Logger LOG = LoggerFactory.getLogger(WorkerSerializeRunnable.class);
    private volatile List<TaskShutdownDameon> shutdownTasks;
    private int threadIndex;
    private int startRunTaskIndex;
    private KryoTupleSerializer serializer;

    public WorkerSerializeRunnable(List<TaskShutdownDameon> shutdownTasks, Map stormConf, GeneralTopologyContext topologyContext, int startRunTaskIndex, int threadIndex) {
        this.shutdownTasks = shutdownTasks;
        this.threadIndex = threadIndex;
        this.startRunTaskIndex = startRunTaskIndex;
        this.serializer = new KryoTupleSerializer(stormConf, topologyContext.getRawTopology());
    }

    @Override
    public String getThreadName() {
        return "worker-serializer-" + threadIndex;
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
        LOG.info("Successfully worker-serializer-{}", threadIndex);
        while (true) {
            try {
                TaskShutdownDameon taskShutdownDameon = shutdownTasks.get(startRunTaskIndex);
                taskShutdownDameon.getTask().getTaskTransfer().serializer(serializer);
                startRunTaskIndex++;
            } catch (IndexOutOfBoundsException e) {
                startRunTaskIndex = 0;
                continue;
            }
        }
    }

    public Object getResult() {
        LOG.info("Begin to shutdown worker-serializer-{}", threadIndex);
        return -1;
    }

}
