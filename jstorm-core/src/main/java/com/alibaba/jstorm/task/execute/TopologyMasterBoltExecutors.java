package com.alibaba.jstorm.task.execute;

import com.alibaba.jstorm.task.Task;

public class TopologyMasterBoltExecutors extends BoltExecutors {
    public TopologyMasterBoltExecutors (Task task) {
        super(task);
    }

    @Override
    public void consumeExecuteQueue() {
        // Consume control queue first, and then async consume execute queue
        controlQueue.consumeBatchWhenAvailable(this);
        exeQueue.consumeBatch(this);
    }
}