package com.alibaba.jstorm.common.metric;

import com.codahale.metrics.Metered;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wange
 * @since 2.1.1
 */
public class MeanMeter implements Metered {

    private final AtomicLong count = new AtomicLong(0L);
    private final long startTime;

    /**
     * Creates a new {@link MeanMeter}.
     */
    public MeanMeter() {
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        count.addAndGet(n);
    }

    @Override
    public long getCount() {
        return count.get();
    }

    @Override
    public double getFifteenMinuteRate() {
        return 0d;
    }

    @Override
    public double getFiveMinuteRate() {
        return 0d;
    }

    @Override
    public double getMeanRate() {
        return calcMean();
    }

    @Override
    public double getOneMinuteRate() {
        return calcMean();
    }

    private double calcMean() {
        double elapsedSec = (System.currentTimeMillis() - startTime) / 1000d;
        if (elapsedSec > 0) {
            return count.get() / elapsedSec;
        }
        return 0;
    }
}
