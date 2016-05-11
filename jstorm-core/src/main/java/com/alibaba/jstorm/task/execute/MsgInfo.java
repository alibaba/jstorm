package com.alibaba.jstorm.task.execute;

import backtype.storm.task.ICollectorCallback;

import java.util.List;

/**
 * Created by xiaojian.fxj on 2015/12/10.
 */
public abstract class MsgInfo {
    public String streamId;
    public Integer outTaskId;
    public List<Object> values;
    public ICollectorCallback callback;

    public MsgInfo(String streamId, List<Object> values, Integer outTaskId) {
        this.streamId = streamId;
        this.values = values;
        this.outTaskId = outTaskId;
        this.callback = null;
    }

    public MsgInfo(String streamId, List<Object> values, Integer outTaskId, ICollectorCallback callback) {
        this.streamId = streamId;
        this.values = values;
        this.outTaskId = outTaskId;
        this.callback = callback;
    }
}