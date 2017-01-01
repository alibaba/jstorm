
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task.master;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.master.ctrlevent.CtrlEventDispatcher;
import com.alibaba.jstorm.task.master.ctrlevent.UpdateConfigEvent;
import com.alibaba.jstorm.task.master.heartbeat.TaskHeartbeatUpdater;
import com.alibaba.jstorm.task.master.metrics.MetricRegister;
import com.alibaba.jstorm.task.master.metrics.MetricsMetaBroadcastEvent;
import com.alibaba.jstorm.task.master.metrics.MetricsUpdater;
import com.alibaba.jstorm.task.master.metrics.MetricsUploader;
import com.alibaba.jstorm.task.master.timer.WorkerSetUpdater;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IDynamicComponent;
import backtype.storm.tuple.Tuple;

/**
 * Topology master is responsible for the process of general topology
 * information, e.g. task heartbeat update, metrics data update....
 *
 * @author Basti Liu
 */
public class TopologyMaster implements IBolt, IDynamicComponent {

    private static final Logger LOG = getLogger(TopologyMaster.class);
    private static final long serialVersionUID = 4690656768333833626L;

    public static final String WORKER_SET_UPDATER_NAME = "worker_set_updater";
    public static final String METRICS_UPLOADER_NAME = "metrics_uploader";
    public static final String MERTRICS_META_BROADCAST = "metrics_meta_broadcast";
    public static final String UPDATE_CONFIG_NAME = "update_config";

    public static final String FIELD_METRIC_WORKER = "worker";
    public static final String FIELD_METRIC_METRICS = "metrics";
    public static final String FIELD_REGISTER_METRICS = "regMetrics";
    public static final String FIELD_REGISTER_METRICS_RESP = "regMetricsResp";
    public static final String FILED_HEARBEAT_EVENT = "hbEvent";
    public static final String FILED_CTRL_EVENT = "ctrlEvent";

    private static int THREAD_POOL_SIZE;
    private TopologyMasterContext tmContext;
    private Map<String, TMHandler> handlers = new ConcurrentHashMap<>();
    private ScheduledExecutorService threadPools;


    public void createThreadPools(Map conf) {
        // It can set thread pool size according to worker number
//		this.threadPools = new ThreadPoolExecutor(THREAD_POOL_SIZE, 
//		        2 * THREAD_POOL_SIZE, 60, TimeUnit.SECONDS, 
//		        new ArrayBlockingQueue<Runnable>(1024),
//		        new ThreadFactory() {
//		    public static final String THREAD_POOL_NAME = "TM-ThreadPool-";
//		    
//		        private AtomicInteger threadCounter = new AtomicInteger();
//                    @Override
//                    public Thread newThread(Runnable r) {
//                        return new Thread(r, THREAD_POOL_NAME + threadCounter.getAndIncrement());
//                    }
//		    
//		});
        THREAD_POOL_SIZE = ConfigExtension.getTopologyMasterThreadPoolSize(conf);
        this.threadPools = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

    }

    public void registerHandlers() {
        // register hb handler
        TMHandler hbHandler = new TaskHeartbeatUpdater();
        hbHandler.init(tmContext);
        handlers.put(Common.TOPOLOGY_MASTER_HB_STREAM_ID, hbHandler);
        handlers.put(UPDATE_CONFIG_NAME, hbHandler);

        // update metric data
        TMHandler metricUpdater = new MetricsUpdater();
        metricUpdater.init(tmContext);
        handlers.put(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID, metricUpdater);

        // update metric meta
        TMHandler metricRegister = new MetricRegister();
        metricRegister.init(tmContext);
        handlers.put(Common.TOPOLOGY_MASTER_REGISTER_METRICS_STREAM_ID, metricRegister);

        // broadcast metric meta to all workers every 15 sec
        handlers.put(MERTRICS_META_BROADCAST, metricRegister);
        TMEvent metricsMetaBroadCastEvent = new TMEvent(metricRegister, new MetricsMetaBroadcastEvent());
        threadPools.scheduleAtFixedRate(metricsMetaBroadCastEvent, 10, 15, TimeUnit.SECONDS);

        // upload metric data every minute
        TMHandler metricsUploader = new MetricsUploader();
        metricsUploader.init(tmContext);
        handlers.put(METRICS_UPLOADER_NAME, metricsUploader);
        TMEvent metricsUploaderEvent = new TMEvent(metricsUploader, null);
        threadPools.scheduleAtFixedRate(metricsUploaderEvent, 5, 60, TimeUnit.SECONDS);

        TMHandler ctrlEventDispatcher = new CtrlEventDispatcher();
        ctrlEventDispatcher.init(tmContext);
        handlers.put(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, ctrlEventDispatcher);
        handlers.put(UPDATE_CONFIG_NAME, ctrlEventDispatcher);

        TMHandler workerSetUpdater = new WorkerSetUpdater();
        workerSetUpdater.init(tmContext);
        handlers.put(WORKER_SET_UPDATER_NAME, workerSetUpdater);
        TMEvent workerSetUpdateEvent = new TMEvent(workerSetUpdater, null);
        threadPools.scheduleAtFixedRate(workerSetUpdateEvent, 10, 10, TimeUnit.SECONDS);

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {

        tmContext = new TopologyMasterContext(stormConf, context, collector);

        createThreadPools(stormConf);

        registerHandlers();

    }

    @Override
    public void execute(Tuple input) {
        TMHandler tmHandler = handlers.get(input.getSourceStreamId());
        if (tmHandler == null) {
            LOG.error("No handler of " + input.getSourceStreamId());
            tmContext.getCollector().fail(input);
            return;
        }

        TMEvent event = new TMEvent(tmHandler, input);
        threadPools.submit(event);
        tmContext.getCollector().ack(input);
    }

    @Override
    public void cleanup() {
        for (Entry<String, TMHandler> entry : handlers.entrySet()) {
            TMHandler handler = entry.getValue();

            handler.cleanup();
        }
        handlers.clear();

        threadPools.shutdownNow();

        LOG.info("Successfully cleanup topology Master");
    }

    @Override
    public void update(Map conf) {
        LOG.info("Topology master received new conf:" + conf);

        TMHandler handler = handlers.get(UPDATE_CONFIG_NAME);
        if (handler == null) {
            LOG.error("No handler to handle update config event");
            return;
        }

        TMEvent event = new TMEvent(handler, new UpdateConfigEvent(conf));
        threadPools.submit(event);

    }

}
