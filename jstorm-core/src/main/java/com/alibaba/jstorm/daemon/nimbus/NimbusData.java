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
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.Config;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.nimbus.ITopologyActionNotifierPlugin;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.BufferInputStream;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.blobstore.AtomicOutputStream;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.config.ConfigUpdateHandler;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.metric.JStormMetricCache;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeCacheMap;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All nimbus data
 */
public class NimbusData {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusData.class);

    /**
     * Due to the conf is no longer to be static, it will be refreshed dynamically
     * it should be AtomicReference
     */
    private final Map<Object, Object> conf;

    private StormClusterState stormClusterState;

    // Map<topologyId, Map<taskid, TkHbCacheTime>>
    private ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache;

    // TODO two kind of value:Channel/BufferFileInputStream
    private TimeCacheMap<Object, Object> downloaders;
    private TimeCacheMap<Object, Object> uploaders;

    private TimeCacheMap<Object, Object> blobDownloaders;
    private TimeCacheMap<Object, Object> blobUploaders;
    private TimeCacheMap<Object, Object> blobListers;
    private BlobStore blobStore;
    private NimbusInfo nimbusHostPortInfo;

    private boolean isLaunchedCleaner;
    private boolean isLaunchedMonitor;

    // cache thrift response to avoid scan zk too frequently
    private NimbusCache nimbusCache;

    private int startTime;

    private final ScheduledExecutorService scheduExec;

    private AtomicInteger submittedCount;

    private StatusTransition statusTransition;

    private static final int SCHEDULE_THREAD_NUM = 12;

    private final INimbus inimubs;

    private final boolean localMode;

    private volatile boolean isLeader;

    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    private ClusterMetricsRunnable metricRunnable;

    // The topologies has been submitted, but the assignment has not finished
    private TimeCacheMap<String, Object> pendingSubmitTopologies;
    private Map<String, Integer> topologyTaskTimeout;

    // Map<TopologyId, TasksHeartbeat>
    private Map<String, TopologyTaskHbInfo> tasksHeartbeat;

    private final JStormMetricCache metricCache;

    private final String clusterName;

    private JStormMetricsReporter metricsReporter;

    private ITopologyActionNotifierPlugin nimbusNotify;

    private final ConfigUpdateHandler configUpdateHandler;

    private ConcurrentHashMap<String, Semaphore> topologyIdtoSem = new ConcurrentHashMap<String, Semaphore>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    public NimbusData(final Map conf, INimbus inimbus) throws Exception {
        this.conf = conf;

        createFileHandler();
        mkBlobCacheMap();
        this.nimbusHostPortInfo = NimbusInfo.fromConf(conf);
        this.blobStore = BlobStoreUtils.getNimbusBlobStore(conf, nimbusHostPortInfo);

        this.isLaunchedCleaner = false;
        this.isLaunchedMonitor = false;

        this.submittedCount = new AtomicInteger(0);

        this.stormClusterState = Cluster.mk_storm_cluster_state(conf);

        createCache();

        this.taskHeartbeatsCache = new ConcurrentHashMap<>();

        this.scheduExec = Executors.newScheduledThreadPool(SCHEDULE_THREAD_NUM);

        this.statusTransition = new StatusTransition(this);

        this.startTime = TimeUtils.current_time_secs();

        this.inimubs = inimbus;

        localMode = StormConfig.local_mode(conf);

        this.metricCache = new JStormMetricCache(conf, this.stormClusterState);
        this.clusterName = ConfigExtension.getClusterName(conf);

        pendingSubmitTopologies = new TimeCacheMap<>(JStormUtils.MIN_10);
        topologyTaskTimeout = new ConcurrentHashMap<>();
        tasksHeartbeat = new ConcurrentHashMap<>();

        // init nimbus metric reporter
        this.metricsReporter = new JStormMetricsReporter(this);

        // metrics thread will be started in NimbusServer
        this.metricRunnable = ClusterMetricsRunnable.mkInstance(this);

        String configUpdateHandlerClass = ConfigExtension.getNimbusConfigUpdateHandlerClass(conf);
        this.configUpdateHandler = (ConfigUpdateHandler) Utils.newInstance(configUpdateHandlerClass);

        if (conf.containsKey(Config.NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN)) {
            String string = (String) conf.get(Config.NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN);
            nimbusNotify = (ITopologyActionNotifierPlugin) Utils.newInstance(string);
        } else {
            nimbusNotify = null;
        }
    }

    public void init() {
        this.metricsReporter.init();
        this.metricRunnable.init();
        this.configUpdateHandler.init(conf);
        if (nimbusNotify != null) {
            nimbusNotify.prepare(conf);
        }
    }

    public void createFileHandler() {
        ExpiredCallback<Object, Object> expiredCallback = new ExpiredCallback<Object, Object>() {
            @Override
            public void expire(Object key, Object val) {
                try {
                    LOG.info("Close file " + String.valueOf(key));
                    if (val != null) {
                        if (val instanceof Channel) {
                            Channel channel = (Channel) val;
                            channel.close();
                        } else if (val instanceof BufferFileInputStream) {
                            BufferFileInputStream is = (BufferFileInputStream) val;
                            is.close();
                        }
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }

            }
        };
        int file_copy_expiration_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
        uploaders = new TimeCacheMap<>(file_copy_expiration_secs, expiredCallback);
        downloaders = new TimeCacheMap<>(file_copy_expiration_secs, expiredCallback);
    }

    public void mkBlobCacheMap() {
        ExpiredCallback<Object, Object> expiredCallback = new ExpiredCallback<Object, Object>() {
            @Override
            public void expire(Object key, Object val) {
                try {
                    LOG.debug("Close blob file " + String.valueOf(key));
                    if (val != null) {
                        if (val instanceof AtomicOutputStream) {
                            AtomicOutputStream stream = (AtomicOutputStream) val;
                            stream.cancel();
                            stream.close();
                        } else if (val instanceof BufferInputStream) {
                            BufferInputStream is = (BufferInputStream) val;
                            is.close();
                        }
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        };

        int expiration_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
        blobUploaders = new TimeCacheMap<>(expiration_secs, expiredCallback);
        blobDownloaders = new TimeCacheMap<>(expiration_secs, expiredCallback);
        blobListers = new TimeCacheMap<>(expiration_secs, null);
    }


    public void createCache() throws IOException {
        nimbusCache = new NimbusCache(conf, stormClusterState);
        ((StormZkClusterState) stormClusterState).setCache(nimbusCache.getMemCache());
    }

    public String getClusterName() {
        return clusterName;
    }

    public int uptime() {
        return (TimeUtils.current_time_secs() - startTime);
    }

    public Map<Object, Object> getConf() {
        return conf;
    }

    public StormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public void setStormClusterState(StormClusterState stormClusterState) {
        this.stormClusterState = stormClusterState;
    }

    public ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> getTaskHeartbeatsCache() {
        return taskHeartbeatsCache;
    }

    public Map<Integer, TkHbCacheTime> getTaskHeartbeatsCache(String topologyId, boolean createIfNotExist) {
        Map<Integer, TkHbCacheTime> ret;
        ret = taskHeartbeatsCache.get(topologyId);
        if (ret == null && createIfNotExist) {
            ret = new ConcurrentHashMap<>();
            Map<Integer, TkHbCacheTime> tmp = taskHeartbeatsCache.putIfAbsent(topologyId, ret);
            if (tmp != null) {
                ret = tmp;
            }
        }
        return ret;
    }

    public void setTaskHeartbeatsCache(ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache) {
        this.taskHeartbeatsCache = taskHeartbeatsCache;
    }

    public TimeCacheMap<Object, Object> getDownloaders() {
        return downloaders;
    }

    public void setDownloaders(TimeCacheMap<Object, Object> downloaders) {
        this.downloaders = downloaders;
    }

    public TimeCacheMap<Object, Object> getUploaders() {
        return uploaders;
    }

    public void setUploaders(TimeCacheMap<Object, Object> uploaders) {
        this.uploaders = uploaders;
    }

    public int getStartTime() {
        return startTime;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public AtomicInteger getSubmittedCount() {
        return submittedCount;
    }

    public ScheduledExecutorService getScheduExec() {
        return scheduExec;
    }

    public StatusTransition getStatusTransition() {
        return statusTransition;
    }

    public void cleanup() {
        nimbusCache.cleanup();
        LOG.info("Successfully shutdown Cache");
        try {
            stormClusterState.disconnect();
            LOG.info("Successfully shutdown ZK Cluster Instance");
        } catch (Exception ignored) {
        }
        try {
            scheduExec.shutdown();
            LOG.info("Successfully shutdown threadpool");
        } catch (Exception ignored) {
        }

        uploaders.cleanup();
        downloaders.cleanup();
        blobUploaders.cleanup();
        blobDownloaders.cleanup();
        blobListers.cleanup();
        blobStore.shutdown();
    }

    public INimbus getInimubs() {
        return inimubs;
    }

    public boolean isLocalMode() {
        return localMode;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public AtomicBoolean getIsShutdown() {
        return isShutdown;
    }

    public JStormCache getMemCache() {
        return nimbusCache.getMemCache();
    }

    public JStormCache getDbCache() {
        return nimbusCache.getDbCache();
    }

    public NimbusCache getNimbusCache() {
        return nimbusCache;
    }

    public JStormMetricCache getMetricCache() {
        return metricCache;
    }

    public TimeCacheMap<String, Object> getPendingSubmitTopologies() {
        return pendingSubmitTopologies;
    }

    public Map<String, Integer> getTopologyTaskTimeout() {
        return topologyTaskTimeout;
    }

    public Map<String, TopologyTaskHbInfo> getTasksHeartbeat() {
        return tasksHeartbeat;
    }

    public ITopologyActionNotifierPlugin getNimbusNotify() {
        return nimbusNotify;
    }

    public TimeCacheMap<Object, Object> getBlobDownloaders() {
        return blobDownloaders;
    }

    public TimeCacheMap<Object, Object> getBlobUploaders() {
        return blobUploaders;
    }

    public TimeCacheMap<Object, Object> getBlobListers() {
        return blobListers;
    }

    public NimbusInfo getNimbusHostPortInfo() {
        return nimbusHostPortInfo;
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    public boolean isLaunchedCleaner() {
        return isLaunchedCleaner;
    }

    public void setLaunchedCleaner(boolean launchedCleaner) {
        isLaunchedCleaner = launchedCleaner;
    }

    public boolean isLaunchedMonitor() {
        return isLaunchedMonitor;
    }

    public void setLaunchedMonitor(boolean launchedMonitor) {
        isLaunchedMonitor = launchedMonitor;
    }

    public ConcurrentHashMap<String, Semaphore> getTopologyIdtoSem() {
        return topologyIdtoSem;
    }
}
