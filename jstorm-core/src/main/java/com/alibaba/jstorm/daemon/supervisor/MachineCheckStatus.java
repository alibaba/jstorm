package com.alibaba.jstorm.daemon.supervisor;

/**
 * Created by dongbin.db on 2016/1/6.
 */
public class MachineCheckStatus {
    public enum StatusType {
        info, warning, error, panic
    }
    private StatusType statusType;

    public MachineCheckStatus() {
        this.statusType = StatusType.info;
    }
    public StatusType getType(){
        return statusType;
    }
    public void updateInfo(){
        statusType = StatusType.info;
    }
    public void updateWarning(){
        statusType = StatusType.warning;
    }
    public void updateError(){
        statusType = StatusType.error;
    }
    public void updatePanic(){
        statusType = StatusType.panic;
    }
    public void SetType(StatusType type){
        this.statusType = type;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MachineCheckStatus status = (MachineCheckStatus) o;

        return statusType == status.statusType;

    }

    @Override
    public int hashCode() {
        return statusType != null ? statusType.hashCode() : 0;
    }
}
