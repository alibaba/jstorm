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

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.transactional.state.ITransactionStateOperator;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The executor which executes windowed bolts.
 *
 * @author Cody
 * @since 2.3.0
 */
public class WindowedBoltExecutor implements IRichBolt, Triggerable {
    private static final long serialVersionUID = 1L;
    protected static final Logger LOG = LoggerFactory.getLogger(WindowedBoltExecutor.class);

    private static final long DEFAULT_MAX_LAG_MS = 0; // no lag
    // min watermark interval, 10ms
    private static final long MIN_WATERMARK_INTERVAL = 10L;
    private static final long DEFAULT_WATERMARK_INTERVAL = 1000L;

    protected final BaseWindowedBolt<Tuple> bolt;
    protected transient OutputCollector collector;

    // window assigner
    protected transient WindowAssigner<Tuple> windowAssigner;
    // state window assigner, it uses the same natural time boundary as window assigner does
    protected transient WindowAssigner<Tuple> stateWindowAssigner;

    protected transient WindowContext windowContext;
    protected transient AccuStateContext accuStateContext;

    // user window states, expires after the window is purged.
    protected transient ConcurrentMap<TimeWindow, Object> userWindowStates;
    // accumulated user window states
    protected transient ConcurrentMap<TimeWindow, Object> accuUserWindowStates;
    // a map from window to state window
    protected transient ConcurrentMap<TimeWindow, TimeWindow> userWindowToStateWindow;
    protected ConcurrentMap<TimeWindow, Trigger<Tuple>> windowToTriggers;

    protected TimeCharacteristic timeCharacteristic;

    // state merger for session time windows
    protected transient WindowStateMerger windowStateMerger;

    // user defined state operator, if set, let user handle all states
    protected ITransactionStateOperator stateOperator;

    // event time
    protected volatile long currentTupleTs;
    protected Retractor retractor;
    protected long maxLagMs;
    protected transient WatermarkTriggerPolicy watermarkTriggerPolicy;
    protected long currentWatermark = Long.MIN_VALUE;
    protected ConcurrentMap<Integer, Long> upstreamWatermarks;
    protected TimestampExtractor timestampExtractor;
    protected double watermarkRatio;

    protected AsmCounter lateTupleCounter;

    /**
     * we have two kinds of watermarks:
     * 1. watermarks sent from upstreams
     * 2. watermark generators bound with current bolt
     *
     * for watermarks sent from upstreams, we use a map to track watermarks of all upstream tasks
     * and always pick the smallest(oldest) watermark as a whole;
     *
     * while for watermark generator, it's generated periodically simply from current bolt,
     * which means there's always only 1 watermark that triggers windows.
     */
    protected transient WatermarkGenerator watermarkGenerator;

    private transient ScheduledThreadPoolExecutor timerTriggerThreadPool;
    protected transient ConcurrentMap<TimeWindow, ScheduledFuture<?>> processingTimeTimerFutures;
    protected transient ConcurrentMap<TimeWindow, ScheduledFuture<?>> eventTimeTimerFutures;

    public WindowedBoltExecutor(BaseWindowedBolt<Tuple> bolt) {
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.windowAssigner = bolt.getWindowAssigner();
        this.stateWindowAssigner = bolt.getStateWindowAssigner();
        this.windowContext = new WindowContext();
        this.accuStateContext = new AccuStateContext();
        this.userWindowStates = new ConcurrentHashMap<>();
        this.accuUserWindowStates = new ConcurrentHashMap<>();
        this.userWindowToStateWindow = new ConcurrentHashMap<>();
        this.windowToTriggers = new ConcurrentHashMap<>();

        if (WindowAssigner.isEventTime(windowAssigner)) {
            this.timeCharacteristic = TimeCharacteristic.EVENT_TIME;
        } else if (WindowAssigner.isProcessingTime(windowAssigner)) {
            this.timeCharacteristic = TimeCharacteristic.PROCESSING_TIME;
        } else if (WindowAssigner.isIngestionTime(windowAssigner)) {
            this.timeCharacteristic = TimeCharacteristic.INGESTION_TIME;
        } else {
            this.timeCharacteristic = TimeCharacteristic.NONE;
        }

        this.windowStateMerger = bolt.getWindowStateMerger();
        this.retractor = bolt.getRetractor();
        this.stateOperator = bolt.getStateOperator();
        if (bolt.getMaxLagMs() > 0) {
            this.maxLagMs = bolt.getMaxLagMs();
        } else {
            this.maxLagMs = DEFAULT_MAX_LAG_MS;
        }

        this.timerTriggerThreadPool = new ScheduledThreadPoolExecutor(4, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("Time Trigger Thread");
                return thread;
            }
        });
        // allow trigger tasks to be removed if all timers for that timestamp are removed by user
        timerTriggerThreadPool.setRemoveOnCancelPolicy(true);

        this.processingTimeTimerFutures = new ConcurrentHashMap<>();
        this.eventTimeTimerFutures = new ConcurrentHashMap<>();
        this.timestampExtractor = bolt.getTimestampExtractor();
        this.watermarkGenerator = bolt.getWatermarkGenerator();
        if (bolt.getWatermarkTriggerPolicy() != null) {
            this.watermarkTriggerPolicy = bolt.getWatermarkTriggerPolicy();
        } else {
            this.watermarkTriggerPolicy = WatermarkTriggerPolicy.TASK_MAX_GLOBAL_MIN_TIMESTAMP;
        }
        if (this.watermarkTriggerPolicy == WatermarkTriggerPolicy.MAX_TIMESTAMP_WITH_RATIO) {
            this.watermarkRatio = JStormUtils.parseDouble(stormConf.get("topology.window.watermark.ratio"), 0.9);
        }

        this.upstreamWatermarks = new ConcurrentHashMap<>();
        Map<String, List<Integer>> sourceCompTasks = context.getThisSourceComponentTasks();
        for (Map.Entry<String, List<Integer>> entry : sourceCompTasks.entrySet()) {
            String comp = entry.getKey();
            List<Integer> tasks = entry.getValue();
            if (!Common.isSystemComponent(comp)) {
                for (Integer task : tasks) {
                    upstreamWatermarks.put(task, Long.MIN_VALUE);
                }
            }
        }

        validate(stormConf);
        bolt.prepare(stormConf, context, this.collector);

        if (timeCharacteristic == TimeCharacteristic.EVENT_TIME) {
            if (watermarkGenerator == null) {
                LOG.info("watermark generator is not set, using default periodic watermark generator with" +
                        " max lag:{}, watermark interval:{}", maxLagMs, DEFAULT_WATERMARK_INTERVAL);
                watermarkGenerator = new PeriodicWatermarkGenerator(
                        Time.milliseconds(maxLagMs), Time.milliseconds(DEFAULT_WATERMARK_INTERVAL));
            }
            watermarkGenerator.init(stormConf, context);
            startWatermarkGenerator();

            MetricClient metricClient = new MetricClient(context);
            this.lateTupleCounter = metricClient.registerCounter("LateTupleNum");
        }
    }

    protected void startWatermarkGenerator() {
        LOG.info("Starting watermark generator");
        long watermarkInterval = watermarkGenerator.getWatermarkInterval();
        if (watermarkInterval < MIN_WATERMARK_INTERVAL) {
            throw new IllegalArgumentException("watermark interval must be greater than 10ms!");
        }

        this.timerTriggerThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long newWatermark = watermarkGenerator.getCurrentWatermark();
                if (newWatermark > currentWatermark) {
                    LOG.info("Generating new watermark:{}", newWatermark);
                    currentWatermark = newWatermark;
                    collector.emit(Common.WATERMARK_STREAM_ID, new Values(new Watermark(newWatermark)));

                    checkEventTimeWindows();
                }
            }
        }, watermarkInterval, watermarkInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void execute(Tuple input) {
        if (timeCharacteristic == TimeCharacteristic.EVENT_TIME &&
                Common.WATERMARK_STREAM_ID.equals(input.getSourceStreamId())) {
            processWatermark(input);
            return;
        }

        if (timeCharacteristic == TimeCharacteristic.EVENT_TIME) {
            currentTupleTs = timestampExtractor.extractTimestamp(input);
            if (currentTupleTs < 0) {
                LOG.error("Extracted a tuple with ts < 0, tuple:{}, dropping...", input);
                return;
            }
        } else if (timeCharacteristic == TimeCharacteristic.PROCESSING_TIME) {
            currentTupleTs = System.currentTimeMillis();
        } else { // ingestion time
            currentTupleTs = ((TupleExt) input).getCreationTimeStamp();
        }
        Collection<TimeWindow> windows = windowAssigner.assignWindows(input, currentTupleTs);
        TimeWindow stateWindow = null;
        if (this.stateWindowAssigner != null) {
            Collection<TimeWindow> stateWindows = stateWindowAssigner.assignWindows(input, currentTupleTs);
            if (stateWindows.size() > 0) {
                stateWindow = stateWindows.iterator().next();
            }
        }
        if (currentTupleTs < currentWatermark) {
            if (retractor != null) {
                retractor.retract(input, windows);
            } else {
                LOG.debug("received a late element with timestamp:{}, current watermark:{}, dropping...",
                        currentTupleTs, currentWatermark);
                lateTupleCounter.inc();
            }
            return;
        }

        if (timeCharacteristic == TimeCharacteristic.EVENT_TIME && watermarkGenerator != null) {
            watermarkGenerator.onElement(currentTupleTs);
        }

        for (TimeWindow window : windows) {
            Object windowState;
            // are we using accumulated state window?
            if (stateWindow != null) {
                windowState = accuUserWindowStates.get(stateWindow);
                if (windowState == null) {
                    windowContext.registerFuture(stateWindow.getEnd(), stateWindow, accuStateContext);

                    windowState = bolt.initWindowState(window);
                    accuUserWindowStates.put(stateWindow, windowState);
                }
                if (!windowToTriggers.containsKey(window)) {
                    createTriggerForWindow(windowAssigner, window, windowToTriggers);
                }
                userWindowToStateWindow.putIfAbsent(window, stateWindow);
            } else {
                windowState = userWindowStates.get(window);
                if (windowState == null) {
                    createTriggerForWindow(windowAssigner, window, windowToTriggers);

                    windowState = bolt.initWindowState(window);
                    userWindowStates.put(window, windowState);
                }
            }
            // user code
            bolt.execute(input, windowState, window);

            Trigger<Tuple> trigger = windowToTriggers.get(window);
            if (trigger == null) {
                throw new RuntimeException("failed to get trigger for window:" + window +
                        " with assigner:" + windowAssigner + ", current ts:" + currentTupleTs);
            }

            // this works for count windows only, because all time-related windows return CONTINUE
            // and windows are triggered by timers
            TriggerResult triggerResult = trigger.onElement(input, currentTupleTs, window, windowContext);
            if (triggerResult == TriggerResult.FIRE) {
                this.trigger(window);
            }
        }

        // merge session windows & states
        if (WindowAssigner.isSessionTime(windowAssigner)) {
            // session windows don't slide
            TimeWindow sessionWindow = windows.iterator().next();
            Set<TimeWindow> oldWindows = Sets.newHashSet(windowToTriggers.keySet());
            for (TimeWindow oldWindow : oldWindows) {
                if (!oldWindow.equals(sessionWindow)) {
                    TimeWindow mergedWindow = mergeSessionWindows(oldWindow, sessionWindow);
                    LOG.debug("merging windows, win1:{}, win2:{}, merged:{}",
                            oldWindow, sessionWindow, mergedWindow);
                    if (!mergedWindow.equals(sessionWindow) && !mergedWindow.equals(oldWindow)) {
                        createTriggerForWindow(windowAssigner, mergedWindow, windowToTriggers);
                    }

                    Object state1 = userWindowStates.get(sessionWindow);
                    Object state2 = userWindowStates.get(oldWindow);
                    Object mergedState = windowStateMerger.reduceState(state1, state2);
                    this.userWindowStates.put(mergedWindow, mergedState);

                    // for late elements, merged window might be contained within the old window or session window
                    if (!mergedWindow.equals(oldWindow)) {
                        removeWindow(oldWindow);
                    }
                    if (!mergedWindow.equals(sessionWindow)) {
                        removeWindow(sessionWindow);
                    }

                    windowToTriggers.get(mergedWindow).onElement(input, currentTupleTs, mergedWindow, windowContext);
                }
            }
        }
    }

    private void processWatermark(Tuple input) {
        long watermark = ((Watermark) input.getValue(0)).getTimestamp();
        // emit watermark to downstream tasks
        collector.emit(Common.WATERMARK_STREAM_ID, new Values(input.getValue(0)));

        Integer taskId = input.getSourceTask();
        Long taskWatermark = upstreamWatermarks.get(taskId);
        if (taskWatermark == null) {
            upstreamWatermarks.put(taskId, watermark);
        } else if (watermark > taskWatermark) {
            upstreamWatermarks.put(taskId, watermark);
        }

        // todo: needs optimization to update watermark, a bit slow for now
        long minWatermark = Collections.min(upstreamWatermarks.values());
        if (minWatermark > currentWatermark) {
            currentWatermark = minWatermark;
            LOG.debug("Updating current watermark to {}({})",
                    currentWatermark, TimeUtils.format((int) (currentWatermark / 1000L)));
        }

        checkEventTimeWindows();
    }

    protected void checkEventTimeWindows() {
        int expectedWatermarks = upstreamWatermarks.size();
        int arrived = 0;
        int good = 0;

        for (Iterator<TimeWindow> windowIterator = windowToTriggers.keySet().iterator();
             windowIterator.hasNext(); ) {
            TimeWindow pendingWindow = windowIterator.next();
            if (watermarkTriggerPolicy == WatermarkTriggerPolicy.GLOBAL_MAX_TIMESTAMP) {
                fireOrReregisterEventWindow(pendingWindow, currentWatermark > pendingWindow.maxTimestamp());
            } else {
                long windowStart = pendingWindow.getStart();
                long windowEnd = pendingWindow.getEnd();
                for (Map.Entry<Integer, Long> entry : upstreamWatermarks.entrySet()) {
                    long ts = entry.getValue();
                    if (ts >= windowStart) {
                        arrived++;
                    }
                    if (ts >= windowEnd) {
                        good++;
                    }
                }

                if (watermarkTriggerPolicy == WatermarkTriggerPolicy.TASK_MAX_GLOBAL_MIN_TIMESTAMP) {
                    fireOrReregisterEventWindow(pendingWindow, good == expectedWatermarks);
                } else if (watermarkTriggerPolicy == WatermarkTriggerPolicy.MAX_TIMESTAMP_WITH_RATIO) {
                    double ratio = good * 1.0d / expectedWatermarks;
                    fireOrReregisterEventWindow(pendingWindow, ratio >= watermarkRatio);
                }
                windowContext.registerEventTimeTimer(pendingWindow.getEnd() + maxLagMs, pendingWindow);
            }
        }
    }

    private void fireOrReregisterEventWindow(TimeWindow pendingWindow, boolean fire) {
        if (fire) {
            LOG.debug("firing event window:{}", pendingWindow);
            windowContext.deleteEventTimeTimer(pendingWindow);
            windowContext.registerEventTimeTimer(0, pendingWindow);
        } else {
            windowContext.registerEventTimeTimer(pendingWindow.getEnd() + maxLagMs, pendingWindow);
        }
    }

    protected void createTriggerForWindow(final WindowAssigner<Tuple> assigner, final TimeWindow window,
                                          final Map<TimeWindow, Trigger<Tuple>> triggerMap) {
        if (assigner instanceof SlidingCountWindows || assigner instanceof TumblingCountWindows) {
            triggerMap.put(window, CountTrigger.<Tuple>of(window.getEnd() - window.getStart()));
        } else if (WindowAssigner.isProcessingTime(assigner)) {
            triggerMap.put(window, ProcessingTimeTrigger.<Tuple>create());
        } else if (WindowAssigner.isEventTime(assigner)) {
            triggerMap.put(window, EventTimeTrigger.<Tuple>create());
        } else if (WindowAssigner.isIngestionTime(assigner)) {
            triggerMap.put(window, IngestionTimeTrigger.<Tuple>create());
        }
    }

    /**
     * merges two windows, if there's an overlap between two windows, return merged window;
     * otherwise return the new window itself.
     */
    private TimeWindow mergeSessionWindows(TimeWindow oldWindow, TimeWindow newWindow) {
        if (oldWindow.intersects(newWindow)) {
            return oldWindow.cover(newWindow);
        }
        return newWindow;
    }

    @Override
    public void trigger(TimeWindow window) {
        Object windowState = null;
        if (stateWindowAssigner != null) {
            TimeWindow stateWindow = userWindowToStateWindow.get(window);
            if (stateWindow != null) {
                windowState = accuUserWindowStates.get(stateWindow);
            }
        }
        if (windowState == null) {
            windowState = userWindowStates.get(window);
        }
        if (windowState == null) {
            LOG.warn("Failed to get window state for window {}, it might has been purged already, skip...", window);
            return;
        }
        // purge window
        bolt.purgeWindow(windowState, window);
        removeWindow(window);
    }

    protected void removeWindow(TimeWindow window) {
        LOG.debug("removing window:{}", window);
        // purge window state
        this.userWindowStates.remove(window);
        this.windowToTriggers.remove(window);
        this.userWindowToStateWindow.remove(window);

        if (timeCharacteristic == TimeCharacteristic.PROCESSING_TIME ||
                timeCharacteristic == TimeCharacteristic.INGESTION_TIME) {
            windowContext.deleteProcessingTimeTimer(window);
        } else if (timeCharacteristic == TimeCharacteristic.EVENT_TIME) {
            windowContext.deleteEventTimeTimer(window);
        }
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
        timerTriggerThreadPool.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    private void validate(Map stormConf) {
        long size = bolt.getSize();
        long slide = bolt.getSlide();
        long stateSize = bolt.getStateSize();

        if (timeCharacteristic == TimeCharacteristic.EVENT_TIME ||
                timeCharacteristic == TimeCharacteristic.PROCESSING_TIME ||
                timeCharacteristic == TimeCharacteristic.INGESTION_TIME) {
            //todo: size must be dividable too
            if (stateSize > 0 && stateSize % size != 0) {
                throw new IllegalArgumentException("state window size and window size must be dividable!");
            }
        } else {
            int maxSpoutPending = getMaxSpoutPending(stormConf);
            ensureCountLessThanMaxPending(size, maxSpoutPending);
            if (slide != BaseWindowedBolt.DEFAULT_SLIDE) {
                ensureCountLessThanMaxPending(slide, maxSpoutPending);
            }
        }
    }

    private int getMaxSpoutPending(Map stormConf) {
        int maxPending = Integer.MAX_VALUE;
        if (stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
            maxPending = ((Number) stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)).intValue();
        }
        return maxPending;
    }

    private void ensureCountLessThanMaxPending(long count, long maxPending) {
        if (count > maxPending) {
            throw new IllegalArgumentException("Window count (length + sliding interval) value " + count +
                    " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING +
                    " value " + maxPending);
        }
    }


    class AccuStateContext implements Triggerable {

        @Override
        public void trigger(TimeWindow window) {
            // when trigger state window, we need to ensure that all user windows before this state window
            // have been purged, otherwise some window may fail to purge because we've already deleted
            // accumulated state.
            boolean delay = false;
            for (TimeWindow pendingWindow : userWindowToStateWindow.keySet()) {
                if (window.getEnd() >= pendingWindow.getEnd()) {
                    delay = true;
                    break;
                }
            }
            if (delay) { // delay 50ms
                windowContext.registerFuture(System.currentTimeMillis() + 50L, window, accuStateContext);
            } else {
                accuUserWindowStates.remove(window);
            }
        }
    }

    class WindowContext implements Trigger.TriggerContext {

        @Override
        public long getMaxLagMs() {
            return maxLagMs;
        }

        @Override
        public long getCurrentWatermark() {
            return WindowedBoltExecutor.this.currentWatermark;
        }

        @Override
        public Collection<TimeWindow> getPendingWindows() {
            return windowToTriggers.keySet();
        }

        @Override
        public void registerProcessingTimeTimer(long time, final TimeWindow window) {
            if (!processingTimeTimerFutures.containsKey(window)) {
                processingTimeTimerFutures.put(window, registerFuture(time, window, WindowedBoltExecutor.this));
            }
        }

        @Override
        public void registerEventTimeTimer(long time, final TimeWindow window) {
            if (!eventTimeTimerFutures.containsKey(window)) {
                eventTimeTimerFutures.put(window, registerFuture(time, window, WindowedBoltExecutor.this));
            }
        }

        @Override
        public ScheduledFuture<?> deleteProcessingTimeTimer(final TimeWindow window) {
            ScheduledFuture<?> future = processingTimeTimerFutures.remove(window);
            if (future != null) {
                future.cancel(true);
            }
            return future;
        }

        @Override
        public ScheduledFuture<?> deleteEventTimeTimer(final TimeWindow window) {
            ScheduledFuture<?> future = eventTimeTimerFutures.remove(window);
            if (future != null) {
                future.cancel(true);
            }
            return future;
        }

        private ScheduledFuture<?> registerFuture(long expectedEnd, final TimeWindow window,
                                                  final Triggerable target) {
            long now = (timeCharacteristic == TimeCharacteristic.EVENT_TIME && currentTupleTs > 0) ?
                    currentTupleTs : System.currentTimeMillis();
            long delay;
            if (expectedEnd <= 0) {
                delay = 0;
            } else {
                // add 5ms delay to avoid the boundary problem
                delay = expectedEnd - now + 5L;
                // expired windows that are restored by previous successful checkpoint
                // should fire immediately
                if (delay < 0) {
                    delay = 0;
                }
            }
            LOG.info("registering future, tuple ts:{}, delay: {}, window: {}",
                    currentTupleTs, delay, window);
            return timerTriggerThreadPool.schedule(new Runnable() {
                @Override
                public void run() {
                    target.trigger(window);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    protected static class Timer implements Comparable<Timer> {
        protected long timestamp;
        protected TimeWindow window;

        public Timer(long timestamp, TimeWindow window) {
            this.timestamp = timestamp;
            this.window = window;
        }

        @Override
        public int compareTo(Timer o) {
            return Long.compare(this.timestamp, o.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Timer timer = (Timer) o;
            return timestamp == timer.timestamp && window.equals(timer.window);
        }

        @Override
        public int hashCode() {
            int result = (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + window.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Timer{" + "timestamp=" + timestamp + ", window=" + window + '}';
        }
    }
}
