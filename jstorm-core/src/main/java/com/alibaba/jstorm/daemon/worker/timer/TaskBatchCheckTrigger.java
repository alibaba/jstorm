package com.alibaba.jstorm.daemon.worker.timer;


import com.alibaba.jstorm.task.TaskBatchTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskBatchCheckTrigger extends TimerTrigger {
    private static final Logger LOG = LoggerFactory.getLogger(TickTupleTrigger.class);

    private TaskBatchTransfer batchTransfer;

    public TaskBatchCheckTrigger(int frequence, String name, TaskBatchTransfer transfer) {
        if (frequence <= 0) {
            LOG.warn(" The frequence of " + name + " is invalid");
            frequence = 1;
        }
        this.firstTime = frequence;
        this.frequence = frequence;
        this.batchTransfer = transfer;
    }

    @Override
    public void run() {
        try {
            //batchTransfer.startCheck();
        } catch (Exception e) {
            LOG.warn("Failed to public timer event to " + name, e);
            return;
        }
    }

}