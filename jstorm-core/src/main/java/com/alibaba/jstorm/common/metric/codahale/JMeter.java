package com.alibaba.jstorm.common.metric.codahale;

import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Metered;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wange
 * @since 15/12/15
 */
public class JMeter implements Metered {
    private static final long TICK_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    private final EWMA m1Rate = EWMA.oneMinuteEWMA();
    private final EWMA m5Rate = EWMA.fiveMinuteEWMA();
    private final EWMA m15Rate = EWMA.fifteenMinuteEWMA();

    private final LongAdder count = new LongAdder();
    private final long startTime;
    private final AtomicLong lastTick;

    /**
     * Creates a new {@link JMeter}.
     */
    public JMeter() {
        this.startTime = System.currentTimeMillis();
        this.lastTick = new AtomicLong(startTime);
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
        tickIfNecessary();
        count.add(n);
        m1Rate.update(n);
        m5Rate.update(n);
        m15Rate.update(n);
    }

    private void tickIfNecessary() {
        final long oldTimeMs = lastTick.get();
        final long newTimeMs = System.currentTimeMillis();
        final long age = newTimeMs - oldTimeMs;
        if (age > TICK_INTERVAL) {
            final long newIntervalStartTick = newTimeMs - age % TICK_INTERVAL;
            if (lastTick.compareAndSet(oldTimeMs, newIntervalStartTick)) {
                final long requiredTicks = age / TICK_INTERVAL;
                for (long i = 0; i < requiredTicks; i++) {
                    m1Rate.tick();
                    m5Rate.tick();
                    m15Rate.tick();
                }
            }
        }
    }

    @Override
    public long getCount() {
        return count.sum();
    }

    @Override
    public double getFifteenMinuteRate() {
        tickIfNecessary();
        return m15Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getFiveMinuteRate() {
        tickIfNecessary();
        return m5Rate.getRate(TimeUnit.SECONDS);
    }

    @Override
    public double getMeanRate() {
        if (getCount() == 0) {
            return 0.0;
        } else {
            final double elapsed = (System.currentTimeMillis() - startTime);
            return getCount() / elapsed * TimeUnit.SECONDS.toMillis(1);
        }
    }

    @Override
    public double getOneMinuteRate() {
        tickIfNecessary();
        return m1Rate.getRate(TimeUnit.SECONDS);
    }


    public static void main(String[] args) {
        int[] speeds = new int[]{0, 0, 0};

//        Meter meter = new Meter();
//        for (int i = 0; i < speeds.length; i++) {
//            for (int j = 0; j < 60; j++) {
//                JStormUtils.sleepMs(1000);
//            }
//            meter.mark(973);
//
//            System.out.println(String.format("m1:%.2f, m5:%.2f, m15:%.2f, mean:%.2f",
//                    meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
//                    meter.getFifteenMinuteRate(), meter.getMeanRate()));
//        }


        JMeter jmeter = new JMeter();
        for (int i = 0; i < speeds.length; i++) {
            for (int j = 0; j < 60; j++) {
                JStormUtils.sleepMs(1000);
            }
            jmeter.mark(973);

            System.out.println(String.format("m1:%.2f, m5:%.2f, m15:%.2f, mean:%.2f",
                    jmeter.getOneMinuteRate(), jmeter.getFiveMinuteRate(),
                    jmeter.getFifteenMinuteRate(), jmeter.getMeanRate()));
        }



        /*
        int[] speeds = new int[]{50, 40, 30, 20, 10, 10, 10, 10};
        for (int i = 0; i < speeds.length; i++) {
            for (int j = 0; j < 60; j++) {
                JStormUtils.sleepMs(1000);
            }
            meter.mark(973);

            System.out.println(String.format("m1:%.2f, m5:%.2f, m15:%.2f, mean:%.2f",
                    meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                    meter.getFifteenMinuteRate(), meter.getMeanRate()));
        }
        */

    }
}
