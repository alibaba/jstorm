package com.alibaba.jstorm.common.metric.codahale;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class JAverageReservoir implements Reservoir {

    private JAverageSnapshot jAverageSnapshot;

    public JAverageReservoir() {
        this.jAverageSnapshot = new JAverageSnapshot();
    }

    @Override
    public int size() {
        // ingore
        return 0;
    }

    @Override
    public void update(long value) {
        // ingore
    }

    @Override
    public Snapshot getSnapshot() {
        return jAverageSnapshot;
    }
}
