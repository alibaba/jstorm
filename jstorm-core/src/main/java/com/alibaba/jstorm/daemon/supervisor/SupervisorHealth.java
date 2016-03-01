package com.alibaba.jstorm.daemon.supervisor;

/**
 * Created by xiaojian.fxj on 2015/12/10.
 */

import backtype.storm.command.health;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by xiaojian.fxj on 2015/11/19.
 */
public class SupervisorHealth extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(SupervisorHealth.class);

    private Map<Object, Object> conf;
    private final int frequence;
    //private volatile HealthStatus healthStatus;
    private volatile MachineCheckStatus checkStatus;

    private long timeOut;
    private String supervisorId;


    public SupervisorHealth(Map conf, MachineCheckStatus status, String supervisorId) {
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.frequence = ConfigExtension.getSupervisorFrequencyCheck(conf);
        this.timeOut = ConfigExtension.getStormHealthTimeoutMs(conf);
        this.active = new AtomicBoolean(true);
        this.checkStatus = status;
        String errorDir = ConfigExtension.getStormMachineResourceErrorCheckDir(conf);
        String panicDir = ConfigExtension.getStormMachineResourcePanicCheckDir(conf);
        String warnDir = ConfigExtension.getStormMachineResourceWarningCheckDir(conf);
        LOG.info("start supervisor health check , timeout is " + timeOut + "," + " scripts directory is: " + panicDir
                + ";" + errorDir + ";" + warnDir);
    }

    @SuppressWarnings("unchecked")
    public void updateStatus() {
        MachineCheckStatus checkStatus = health.check();
        //HealthStatus status = health.healthCheck();
        this.checkStatus.SetType(checkStatus.getType());
    }

    private AtomicBoolean active = null;

    private Integer result;

    @Override
    public Object getResult() {
        if (active.get()) {
            result = frequence;
        } else {
            result = -1;
        }
        return result;
    }

    @Override
    public void run() {
        updateStatus();
    }
    public void setActive(boolean active ){
        this.active.getAndSet(active);
    }
}
