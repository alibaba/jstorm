/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.window;

import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.transactional.state.ITransactionStateOperator;

/**
 * @author wange
 * @since 16/12/15
 */
public abstract class BaseWindowedBolt<T extends Tuple> implements IWindowedBolt<T> {
    private static final long serialVersionUID = 1L;
    public static final long DEFAULT_SLIDE = -1L;
    public static final long DEFAULT_STATE_SIZE = -1L;

    private WindowAssigner<T> windowAssigner;
    private WindowAssigner<T> stateWindowAssigner;
    private WatermarkGenerator watermarkGenerator;
    private TimestampExtractor timestampExtractor;
    private WindowStateMerger windowStateMerger;
    private Retractor retractor;
    private ITransactionStateOperator stateOperator;
    private WatermarkTriggerPolicy watermarkTriggerPolicy;

    private long maxLagMs;
    private long size;
    private long slide;
    private long stateSize = DEFAULT_STATE_SIZE;

    /**
     * define a tumbling count window
     *
     * @param size count size
     */
    public BaseWindowedBolt<T> countWindow(long size) {
        ensurePositiveTime(size);

        setSizeAndSlide(size, DEFAULT_SLIDE);
        this.windowAssigner = TumblingCountWindows.create(size);
        return this;
    }

    /**
     * define a sliding count window
     *
     * @param size  count size
     * @param slide slide size
     */
    public BaseWindowedBolt<T> countWindow(long size, long slide) {
        ensurePositiveTime(size, slide);
        ensureSizeGreaterThanSlide(size, slide);

        setSizeAndSlide(size, slide);
        this.windowAssigner = SlidingCountWindows.create(size, slide);
        return this;
    }

    /**
     * define a tumbling processing time window
     *
     * @param size window size
     */
    public BaseWindowedBolt<T> timeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = TumblingProcessingTimeWindows.of(s);
        return this;
    }


    /**
     * add state window to tumbling windows.
     * If set, jstorm will use state window size to accumulate user states instead of
     * deleting the states right after purging a window.
     *
     * e.g., if a user defines a window of 10 sec as well as a state window of 60 sec,
     * then the window will be purged every 10 sec, while the state of the window will be kept (and accumulated)
     * until 60 sec is due.
     *
     * @param size state window size
     */
    public BaseWindowedBolt<T> withStateSize(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);
        ensureStateSizeGreaterThanWindowSize(this.size, s);

        this.stateSize = s;
        if (WindowAssigner.isEventTime(this.windowAssigner)) {
            this.stateWindowAssigner = TumblingEventTimeWindows.of(s);
        } else if (WindowAssigner.isProcessingTime(this.windowAssigner)) {
            this.stateWindowAssigner = TumblingProcessingTimeWindows.of(s);
        } else if (WindowAssigner.isIngestionTime(this.windowAssigner)) {
            this.stateWindowAssigner = TumblingIngestionTimeWindows.of(s);
        }

        return this;
    }

    /**
     * define a sliding processing time window
     *
     * @param size  window size
     * @param slide slide size
     */
    public BaseWindowedBolt<T> timeWindow(Time size, Time slide) {
        long s = size.toMilliseconds();
        long l = slide.toMilliseconds();
        ensurePositiveTime(s, l);
        ensureSizeGreaterThanSlide(s, l);

        setSizeAndSlide(s, l);
        this.windowAssigner = SlidingProcessingTimeWindows.of(s, l);
        return this;
    }

    /**
     * define a tumbling ingestion time window
     *
     * @param size window size
     */
    public BaseWindowedBolt<T> ingestionTimeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = TumblingIngestionTimeWindows.of(s);
        return this;
    }

    /**
     * define a sliding ingestion time window
     *
     * @param size  window size
     * @param slide slide size
     */
    public BaseWindowedBolt<T> ingestionTimeWindow(Time size, Time slide) {
        long s = size.toMilliseconds();
        long l = slide.toMilliseconds();
        ensurePositiveTime(s, l);
        ensureSizeGreaterThanSlide(s, l);

        setSizeAndSlide(s, l);
        this.windowAssigner = SlidingIngestionTimeWindows.of(s, l);
        return this;
    }

    /**
     * define a tumbling event time window
     *
     * @param size window size
     */
    public BaseWindowedBolt<T> eventTimeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = TumblingEventTimeWindows.of(s);
        return this;
    }

    /**
     * define a sliding event time window
     *
     * @param size  window size
     * @param slide slide size
     */
    public BaseWindowedBolt<T> eventTimeWindow(Time size, Time slide) {
        long s = size.toMilliseconds();
        long l = slide.toMilliseconds();
        ensurePositiveTime(s, l);
        ensureSizeGreaterThanSlide(s, l);

        setSizeAndSlide(s, l);
        this.windowAssigner = SlidingEventTimeWindows.of(s, l);
        return this;
    }


    /**
     * define a session processing time window
     *
     * @param size window size, i.e., session gap
     */
    public BaseWindowedBolt<T> sessionTimeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = ProcessingTimeSessionWindows.withGap(s);
        return this;
    }

    /**
     * define a session event time window
     *
     * @param size window size, i.e., session gap
     */
    public BaseWindowedBolt<T> sessionEventTimeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = EventTimeSessionWindows.withGap(s);
        return this;
    }

    /**
     * define a timestamp extractor, only for event time windows
     *
     * @param timestampExtractor timestamp extractor implementation
     */
    public BaseWindowedBolt<T> withTimestampExtractor(TimestampExtractor timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
        return this;
    }

    /**
     * define a watermark generator, only for event time windows
     *
     * @param watermarkGenerator watermark generator
     */
    public BaseWindowedBolt<T> withWatermarkGenerator(WatermarkGenerator watermarkGenerator) {
        this.watermarkGenerator = watermarkGenerator;
        return this;
    }

    /**
     * define a watermark trigger policy, only for event time windows
     *
     * @param triggerPolicy watermark trigger policy
     */
    public BaseWindowedBolt<T> withWatermarkTriggerPolicy(WatermarkTriggerPolicy triggerPolicy) {
        this.watermarkTriggerPolicy = triggerPolicy;
        return this;
    }

    /**
     * define a window state merger, only for session time windows
     */
    public BaseWindowedBolt<T> withWindowStateMerger(WindowStateMerger merger) {
        this.windowStateMerger = merger;
        return this;
    }

    /**
     * define max lag in ms, only for event time windows
     *
     * @param maxLag max lag time
     */
    public BaseWindowedBolt<T> withMaxLagMs(Time maxLag) {
        this.maxLagMs = maxLag.toMilliseconds();
        ensureNonNegativeTime(maxLagMs);
        return this;
    }

    /**
     * define a retractor which enables re-computing of historic windows, only for event time windows
     *
     * @param retractor retractor
     */
    public BaseWindowedBolt<T> withRetractor(Retractor retractor) {
        this.retractor = retractor;
        return this;
    }

    public BaseWindowedBolt<T> withTransactionStateOperator(ITransactionStateOperator stateOperator) {
        this.stateOperator = stateOperator;
        return this;
    }

    public WindowAssigner<T> getWindowAssigner() {
        return this.windowAssigner;
    }

    public WindowAssigner<T> getStateWindowAssigner() {
        return stateWindowAssigner;
    }

    public WatermarkGenerator getWatermarkGenerator() {
        return this.watermarkGenerator;
    }

    public WatermarkTriggerPolicy getWatermarkTriggerPolicy() {
        return this.watermarkTriggerPolicy;
    }

    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }

    public WindowStateMerger getWindowStateMerger() {
        return windowStateMerger;
    }

    public long getMaxLagMs() {
        return maxLagMs;
    }

    public Retractor getRetractor() {
        return retractor;
    }

    public ITransactionStateOperator getStateOperator() {
        return this.stateOperator;
    }

    private void setSizeAndSlide(long size, long slide) {
        this.size = size;
        this.slide = slide;
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    public long getStateSize() {
        return stateSize;
    }

    private void ensurePositiveTime(long... values) {
        for (long value : values) {
            if (value <= 0) {
                throw new IllegalArgumentException("time or slide must be positive!");
            }
        }
    }

    private void ensureSizeGreaterThanSlide(long size, long slide) {
        if (size <= slide) {
            throw new IllegalArgumentException("window size must be greater than window slide!");
        }
    }

    private void ensureStateSizeGreaterThanWindowSize(long winSize, long stateSize) {
        if (winSize > stateSize) {
            throw new IllegalArgumentException("state window size must be greater than window size!");
        }
    }

    private void ensureNonNegativeTime(long... values) {
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("time or slide must not be negative!");
            }
        }
    }

    @Override
    public void cleanup() {
    }
}
