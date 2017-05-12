package com.alibaba.jstorm.window;

import java.util.Collection;

public interface IWindowedTxStateOperator {
    public void initState(Collection<TimeWindow> window, Object userState);

    public Object finishBatch(long batchId, Collection<TimeWindow> window);

    public Object commit(long batchId, Collection<TimeWindow> window, Object state);

    public void rollBack(Collection<TimeWindow> window, Object userState);

    public void ackCommit(long batchId, Collection<TimeWindow> window, long timeStamp);
}