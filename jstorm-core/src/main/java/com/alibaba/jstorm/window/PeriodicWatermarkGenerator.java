package com.alibaba.jstorm.window;

import backtype.storm.task.TopologyContext;
import java.util.Map;

/**
 * Periodic watermark generator, used in WindowedBolt when upstream/source doesn't send watermarks.
 * Use code like following to register a watermark generator:
 * <pre>{@code
 *  BaseWindowedBolt windowedBolt = new YourBolt();
 *  windowedBolt.withWatermarkGenerator(new PeriodicWatermarkGenerator());
 * }
 * </pre>
 *
 * Note that periodic watermark generator might be quite inaccurate about real watermarks since it has no context
 * about data source. A better approach would be sending watermarks from spout manually, or track upstream watermarks
 * so that you have a big picture about the progress of upstream flow.
 *
 * @author Cody
 * @since 2.3.0
 */
public class PeriodicWatermarkGenerator implements WatermarkGenerator {

    private static final long serialVersionUID = 1L;

    private long maxTimestamp = Long.MIN_VALUE;
    private long maxLagMs = 0L;
    private long watermarkInterval;

    public PeriodicWatermarkGenerator(Time maxLag, Time triggerPeriod) {
        this.maxLagMs = maxLag.toMilliseconds();
        this.watermarkInterval = triggerPeriod.toMilliseconds();
    }

    @Override
    public void init(Map conf, TopologyContext context) {
    }

    @Override
    public long getCurrentWatermark() {
        if (maxTimestamp < 0) {
            return 0L;
        }
        long ts = maxTimestamp - maxLagMs;
        if (ts < 0) {
            return 0L;
        }
        return ts;
    }

    @Override
    public void onElement(long timestamp) {
        if (timestamp > maxTimestamp) {
            this.maxTimestamp = timestamp;
        }
    }

    @Override
    public long getWatermarkInterval() {
        return this.watermarkInterval;
    }
}