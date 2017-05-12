package com.alibaba.jstorm.task.upgrade;

import com.alibaba.jstorm.utils.TimeUtils;
import java.io.Serializable;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @author wange
 * @since 2.3.1
 */
public class GrayUpgradeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private long upgradeExpireTime;

    // worker selection priority: workers > component > workerNum
    // note that component can co-exist with workerNum
    // if component is not empty and workerNum==0, this means to upgrade all
    // workers within this component
    private int workerNum;
    private String component;
    private Set<String> workers;
    private long completeTime;

    private boolean continueUpgrade = false;
    private boolean rollback = false;

    public GrayUpgradeConfig setUpgradeExpireTime(long upgradeExpireTime) {
        this.upgradeExpireTime = upgradeExpireTime;
        return this;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public GrayUpgradeConfig setWorkerNum(int workerNum) {
        this.workerNum = workerNum;
        return this;
    }

    public boolean continueUpgrading() {
        return continueUpgrade;
    }

    public GrayUpgradeConfig setContinueUpgrade(boolean continueUpgrade) {
        this.continueUpgrade = continueUpgrade;
        return this;
    }

    public boolean isRollback() {
        return rollback;
    }

    public GrayUpgradeConfig setRollback(boolean rollback) {
        this.rollback = rollback;
        return this;
    }

    public GrayUpgradeConfig setCompleteTime(long completeTime) {
        this.completeTime = completeTime;
        return this;
    }

    public boolean isCompleted() {
        return completeTime > 0 && completeTime < System.currentTimeMillis();
    }

    public boolean isExpired() {
        return upgradeExpireTime < System.currentTimeMillis();
    }

    public String getComponent() {
        return component;
    }

    public GrayUpgradeConfig setComponent(String component) {
        this.component = component;
        return this;
    }

    public Set<String> getWorkers() {
        return workers;
    }

    public GrayUpgradeConfig setWorkers(Set<String> workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public static void completeUpgrade(GrayUpgradeConfig config) {
        config.setCompleteTime(TimeUtils.current_time_secs());
    }
}

