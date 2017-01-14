
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

import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.utils.LoadConf;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.blobstore.AtomicOutputStream;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.blobstore.InputStreamWithMeta;
import com.alibaba.jstorm.blobstore.LocalFsBlobStore;
import com.alibaba.jstorm.callback.impl.RemoveTransitionCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.DaemonCommon;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.KillTopologyEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.assignment.StartTopologyEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.update.UpdateEvent;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.error.TaskError;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.Thrift;
import com.alibaba.jstorm.utils.TimeCacheMap;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.BeginDownloadResult;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.Credentials;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.ListBlobsResult;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.MonitorOptions;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NimbusSummary;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.TaskComponent;
import backtype.storm.generated.TaskHeartbeat;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.nimbus.ITopologyActionNotifierPlugin;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.BufferInputStream;
import backtype.storm.utils.Utils;

/**
 * Thrift callback, all commands handling entrance
 *
 * @author version 1: lixin, version 2:Longda
 */
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
    private final static Logger LOG = LoggerFactory.getLogger(ServiceHandler.class);

    public final static int THREAD_NUM = 64;

    private NimbusData data;

    private Map<Object, Object> conf;

    public ServiceHandler(NimbusData data) {
        this.data = data;
        conf = data.getConf();
    }

    /**
     * Shutdown the nimbus
     */
    @Override
    public void shutdown() {
        LOG.info("Begin to shut down master");
        // Timer.cancelTimer(nimbus.getTimer());
        ITopologyActionNotifierPlugin nimbusNotify = data.getNimbusNotify();
        if (nimbusNotify != null)
            nimbusNotify.cleanup();
        LOG.info("Successfully shut down master");

    }

    @Override
    public boolean waiting() {
        // @@@ TODO
        return false;
    }

    @Override
    public String submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology) throws TException, AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
        SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
        return submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, options);
    }

    private void makeAssignment(String topologyName, String topologyId, TopologyInitialStatus status) throws FailedAssignTopologyException {
        TopologyAssignEvent assignEvent = new TopologyAssignEvent();
        assignEvent.setTopologyId(topologyId);
        assignEvent.setScratch(false);
        assignEvent.setTopologyName(topologyName);
        assignEvent.setOldStatus(Thrift.topologyInitialStatusToStormStatus(status));

        TopologyAssign.push(assignEvent);

        boolean isSuccess = assignEvent.waitFinish();
        if (isSuccess == true) {
            LOG.info("Finish submit for " + topologyName);
        } else {
            throw new FailedAssignTopologyException(assignEvent.getErrorMsg());
        }
    }

    /**
     * Submit one Topology
     *
     * @param topologyName        String: topology name
     * @param uploadedJarLocation String: already uploaded jar path
     * @param jsonConf            String: jsonConf serialize all toplogy configuration to
     *                            Json
     * @param topology            StormTopology: topology Object
     */
    @SuppressWarnings("unchecked")
    @Override
    public String submitTopologyWithOpts(String topologyName, String uploadedJarLocation, String jsonConf, StormTopology topology, SubmitOptions options)
            throws AlreadyAliveException, InvalidTopologyException, TopologyAssignException, TException {
        LOG.info("Receive " + topologyName + ", uploadedJarLocation:" + uploadedJarLocation);

        long start = System.nanoTime();

        //check topologyname is valid
        if (!Common.charValidate(topologyName)) {
            throw new InvalidTopologyException(topologyName + " is not a valid topology name");
        }

        Map<Object, Object> serializedConf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);
        if (serializedConf == null) {
            LOG.warn("Failed to serialized Configuration");
            throw new InvalidTopologyException("Failed to serialize topology configuration");
        }

        Common.confValidate(serializedConf, data.getConf());

        boolean enableDeploy = ConfigExtension.getTopologyHotDeplogyEnable(serializedConf);

        try {
            checkTopologyActive(data, topologyName, enableDeploy);
        } catch (AlreadyAliveException e) {
            LOG.info(topologyName + " already exists ");
            throw e;
        } catch (NotAliveException e){
            LOG.info(topologyName + " is not alive ");
            throw e;
        }catch (Throwable e) {
            LOG.info("Failed to check whether topology is alive or not", e);
            throw new TException(e);
        }
        if (enableDeploy){
            LOG.info("deploy the topology");
            try {
                StormClusterState stormClusterState = data.getStormClusterState();
                String topologyId = Cluster.get_topology_id(stormClusterState, topologyName);
                if (topologyId == null) {
                    throw new NotAliveException(topologyName);
                }
                LOG.info("start kill the old  topology {}", topologyId);
                Map oldConf = new HashMap();
                oldConf.putAll(conf);
                Map killedStormConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
                if (killedStormConf != null) {
                    oldConf.putAll(killedStormConf);
                }

                NimbusUtils.transitionName(data, topologyName, true, StatusType.kill, 0);
                KillTopologyEvent.pushEvent(topologyId);
                notifyTopologyActionListener(topologyName, "killTopology");
                //wait all workers' are killed
                final long timeoutSeconds = ConfigExtension.getTaskCleanupTimeoutSec(oldConf);
                ConcurrentHashMap<String, Semaphore> topologyIdtoSem = data.getTopologyIdtoSem();
                if (!topologyIdtoSem.contains(topologyId)){
                    topologyIdtoSem.putIfAbsent(topologyId, new Semaphore(0));
                }
                Semaphore semaphore = topologyIdtoSem.get(topologyId);
                if (semaphore != null){
                    semaphore.tryAcquire(timeoutSeconds, TimeUnit.SECONDS);
                    topologyIdtoSem.remove(semaphore);
                }
                LOG.info("success kill the old topology {}", topologyId);

            } catch (Exception e) {
                String errMsg = "Failed to kill topology " + topologyName;
                LOG.error(errMsg, e);
                throw new TException(errMsg);
            }
        }
        String topologyId = null;
        synchronized (data) {
            // avoid same topologies from being submitted at the same time
            Set<String> pendingTopologies =
                    data.getPendingSubmitTopologies().buildMap().keySet();
            for (String cachTopologyId : pendingTopologies) {
                if (cachTopologyId.contains(topologyName + "-"))
                    throw new AlreadyAliveException(
                            topologyName + "  were submitted");
            }
            int counter = data.getSubmittedCount().incrementAndGet();
            topologyId = Common.topologyNameToId(topologyName, counter);
            data.getPendingSubmitTopologies().put(topologyId, null);
        }
        try {

            serializedConf.put(Config.TOPOLOGY_ID, topologyId);
            serializedConf.put(Config.TOPOLOGY_NAME, topologyName);

            Map<Object, Object> stormConf;

            stormConf = NimbusUtils.normalizeConf(conf, serializedConf, topology);
            LOG.info("Normalized configuration:" + stormConf);

            Map<Object, Object> totalStormConf = new HashMap<Object, Object>(conf);
            totalStormConf.putAll(stormConf);

            StormTopology normalizedTopology = NimbusUtils.normalizeTopology(stormConf, topology, true);

            // this validates the structure of the topology
            Common.validate_basic(normalizedTopology, totalStormConf, topologyId);
            // don't need generate real topology, so skip Common.system_topology
            // Common.system_topology(totalStormConf, topology);

            StormClusterState stormClusterState = data.getStormClusterState();

            // create /local-dir/nimbus/topologyId/xxxx files
            setupStormCode(conf, topologyId, uploadedJarLocation, stormConf, normalizedTopology);
            // wait for blob replication before activate topology
            waitForDesiredCodeReplication(conf, topologyId);
            // generate TaskInfo for every bolt or spout in ZK
            // /ZK/tasks/topoologyId/xxx
            setupZkTaskInfo(conf, topologyId, stormClusterState);

            //mkdir topology error directory
            String path = Cluster.taskerror_storm_root(topologyId);
            stormClusterState.mkdir(path);

            // make assignments for a topology
            LOG.info("Submit for " + topologyName + " with conf " + serializedConf);
            makeAssignment(topologyName, topologyId, options.get_initial_status());

            // push start event after startup
            double metricsSampleRate = ConfigExtension.getMetricSampleRate(stormConf);
            StartTopologyEvent.pushEvent(topologyId, metricsSampleRate);

            notifyTopologyActionListener(topologyName, "submitTopology");

        } catch (FailedAssignTopologyException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology, Root cause:");
            if (e.getMessage() == null) {
                sb.append("submit timeout");
            } else {
                sb.append(e.getMessage());
            }

            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            throw new TopologyAssignException(sb.toString());
        } catch (InvalidParameterException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology ");
            sb.append(e.getMessage());
            sb.append(", cause:" + e.getCause());
            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            throw new InvalidParameterException(sb.toString());
        } catch (InvalidTopologyException e) {
            LOG.error("Topology is invalid. " + e.get_msg());
            throw e;
        } catch (Throwable e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Fail to sumbit topology ");
            sb.append(e.getMessage());
            sb.append(", cause:" + e.getCause());
            sb.append("\n\n");
            sb.append("topologyId:" + topologyId);
            sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
            LOG.error(sb.toString(), e);
            throw new TopologyAssignException(sb.toString());
        } finally {
            // when make assignment for a topology,so remove the topologyid form
            // pendingSubmitTopologys
            data.getPendingSubmitTopologies().remove(topologyId);
            double spend = (System.nanoTime() - start) / TimeUtils.NS_PER_US;
            SimpleJStormMetric.updateNimbusHistogram("submitTopologyWithOpts", spend);
            LOG.info("submitTopologyWithOpts {} costs {}ms", topologyName, spend);
        }
        return topologyId;

    }

    /**
     * kill topology
     *
     * @param topologyName String topology name
     */
    @Override
    public void killTopology(String topologyName) throws TException, NotAliveException {
        killTopologyWithOpts(topologyName, new KillOptions());

    }

    @Override
    public void killTopologyWithOpts(String topologyName, KillOptions options) throws TException, NotAliveException {
        try {
            checkTopologyActive(data, topologyName, true);

            String topologyId = getTopologyId(topologyName);

            Integer wait_amt = null;
            if (options.is_set_wait_secs()) {
                wait_amt = options.get_wait_secs();
            }
            NimbusUtils.transitionName(data, topologyName, true, StatusType.kill, wait_amt);

            KillTopologyEvent.pushEvent(topologyId);
            notifyTopologyActionListener(topologyName, "killTopology");
        } catch (NotAliveException e) {
            String errMsg = "KillTopology Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to kill topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * set topology status as active
     *
     * @param topologyName
     */
    @Override
    public void activate(String topologyName) throws TException, NotAliveException {
        try {
            NimbusUtils.transitionName(data, topologyName, true, StatusType.activate);
            notifyTopologyActionListener(topologyName, "activate");
        } catch (NotAliveException e) {
            String errMsg = "Activate Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to active topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * set topology stauts as deactive
     *
     * @param topologyName
     */
    @Override
    public void deactivate(String topologyName) throws TException, NotAliveException {
        try {
            NimbusUtils.transitionName(data, topologyName, true, StatusType.inactivate);
            notifyTopologyActionListener(topologyName, "inactivate");
        } catch (NotAliveException e) {
            String errMsg = "Deactivate Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to deactivate topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    /**
     * rebalance one topology
     *
     * @param topologyName topology name
     * @param options      RebalanceOptions
     * @@@ rebalance options hasn't implements
     * <p/>
     * It is used to let workers wait several seconds to finish jobs
     */
    @Override
    public void rebalance(String topologyName, RebalanceOptions options) throws TException, NotAliveException {
        try {
            checkTopologyActive(data, topologyName, true);
            Integer wait_amt = null;
            String jsonConf = null;
            Boolean reassign = false;
            if (options != null) {
                if (options.is_set_wait_secs())
                    wait_amt = options.get_wait_secs();
                if (options.is_set_reassign())
                    reassign = options.is_reassign();
                if (options.is_set_conf())
                    jsonConf = options.get_conf();
            }

            LOG.info("Begin to rebalance " + topologyName + "wait_time:" + wait_amt + ", reassign: " + reassign + ", new worker/bolt configuration:" + jsonConf);

            Map<Object, Object> conf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);

            NimbusUtils.transitionName(data, topologyName, true, StatusType.rebalance, wait_amt, reassign, conf);

            notifyTopologyActionListener(topologyName, "rebalance");
        } catch (NotAliveException e) {
            String errMsg = "Rebalance Error, no this topology " + topologyName;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to rebalance topology " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    @Override
    public void restart(String name, String jsonConf) throws TException, NotAliveException, InvalidTopologyException, TopologyAssignException {
        LOG.info("Begin to restart " + name + ", new configuration:" + jsonConf);

        // 1. get topologyId
        StormClusterState stormClusterState = data.getStormClusterState();
        String topologyId;
        try {
            topologyId = Cluster.get_topology_id(stormClusterState, name);
        } catch (Exception e2) {
            topologyId = null;
        }
        if (topologyId == null) {
            LOG.info("No topology of " + name);
            throw new NotAliveException("No topology of " + name);
        }

        // Restart the topology: Deactivate -> Kill -> Submit
        // 2. Deactivate
        deactivate(name);
        JStormUtils.sleepMs(5000);
        LOG.info("Deactivate " + name);

        // 3. backup old jar/configuration/topology
        StormTopology topology;
        Map topologyConf;
        String topologyCodeLocation = null;
        try {
            topology = StormConfig.read_nimbus_topology_code(topologyId, data.getBlobStore());
            topologyConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
            if (jsonConf != null) {
                Map<Object, Object> newConf = (Map<Object, Object>) JStormUtils.from_json(jsonConf);
                topologyConf.putAll(newConf);
            }

            // copy storm files back to inbox from blob store
            String parent = StormConfig.masterInbox(conf);
            topologyCodeLocation = parent + PathUtils.SEPERATOR + topologyId;
            FileUtils.forceMkdir(new File(topologyCodeLocation));
            FileUtils.cleanDirectory(new File(topologyCodeLocation));
            copyBackToInbox(topologyId, topologyCodeLocation);

            LOG.info("Successfully read old jar/conf/topology " + name);
            notifyTopologyActionListener(name, "restart");
        } catch (Exception e) {
            LOG.error("Failed to read old jar/conf/topology", e);
            if (topologyCodeLocation != null) {
                try {
                    PathUtils.rmr(topologyCodeLocation);
                } catch (IOException ignored) {
                }
            }
            throw new TException("Failed to read old jar/conf/topology ");

        }

        // 4. Kill
        // directly use remove command to kill, more stable than issue kill cmd
        RemoveTransitionCallback killCb = new RemoveTransitionCallback(data, topologyId);
        killCb.execute(new Object[0]);
        LOG.info("Successfully kill the topology " + name);

        // send metric events
        KillTopologyEvent.pushEvent(topologyId);

        // 5. submit
        try {
            submitTopology(name, topologyCodeLocation, JStormUtils.to_json(topologyConf), topology);
        } catch (AlreadyAliveException e) {
            LOG.info("Failed to kill the topology" + name);
            throw new TException("Failed to kill the topology" + name);
        } finally {
            try {
                PathUtils.rmr(topologyCodeLocation);
            } catch (IOException ignored) {
            }
        }

    }

    private void copyBackToInbox(String topologyId, String topologyCodeLocation) throws Exception {
        String codeKey = StormConfig.master_stormcode_key(topologyId);
        String confKey = StormConfig.master_stormconf_key(topologyId);
        String jarKey = StormConfig.master_stormjar_key(topologyId);

        data.getBlobStore().readBlobTo(codeKey, new FileOutputStream(StormConfig.stormcode_path(topologyCodeLocation)));
        data.getBlobStore().readBlobTo(confKey, new FileOutputStream(StormConfig.stormconf_path(topologyCodeLocation)));
        data.getBlobStore().readBlobTo(jarKey, new FileOutputStream(StormConfig.stormjar_path(topologyCodeLocation)));

        try {
            Map stormConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
            if (stormConf != null) {
                List<String> libs = (List<String>) stormConf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
                FileUtils.forceMkdir(new File(topologyCodeLocation + File.separator + "lib"));
                if (libs != null) {
                    for (String libName : libs) {
                        String libKey = StormConfig.master_stormlib_key(topologyId, libName);
                        data.getBlobStore().readBlobTo(libKey,
                                new FileOutputStream(StormConfig.stormlib_path(topologyCodeLocation, libName)));
                    }
                }
            }
        } catch (KeyNotFoundException e) {
            LOG.warn("can't find conf of topology {}", topologyId);
        }
    }


    @Override
    public void beginLibUpload(String libName) throws TException {
        try {
            String parent = PathUtils.parent_path(libName);
            PathUtils.local_mkdirs(parent);
            data.getUploaders().put(libName, Channels.newChannel(new FileOutputStream(libName)));
            LOG.info("Begin upload file from client to " + libName);
        } catch (Exception e) {
            LOG.error("Fail to upload jar " + libName, e);
            throw new TException(e);
        }
    }

    /**
     * prepare to uploading topology jar, return the file location
     */
    @Override
    public String beginFileUpload() throws TException {

        String fileLoc = null;
        try {
            String path;
            String key = UUID.randomUUID().toString();
            path = StormConfig.masterInbox(conf) + "/" + key;
            FileUtils.forceMkdir(new File(path));
            FileUtils.cleanDirectory(new File(path));
            fileLoc = path + "/stormjar-" + key + ".jar";

            data.getUploaders().put(fileLoc, Channels.newChannel(new FileOutputStream(fileLoc)));
            LOG.info("Begin upload file from client to " + fileLoc);
            return path;
        } catch (FileNotFoundException e) {
            LOG.error("File not found: " + fileLoc, e);
            throw new TException(e);
        } catch (IOException e) {
            LOG.error("Upload file error: " + fileLoc, e);
            throw new TException(e);
        }
    }

    /**
     * uploading topology jar data
     */
    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws TException {
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException("File for that location does not exist (or timed out) " + location);
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.write(chunk);
                uploaders.put(location, channel);
            } else {
                throw new TException("Object isn't WritableByteChannel for " + location);
            }
        } catch (IOException e) {
            String errMsg = " WritableByteChannel write filed when uploadChunk " + location;
            LOG.error(errMsg);
            throw new TException(e);
        }

    }

    @Override
    public void finishFileUpload(String location) throws TException {
        TimeCacheMap<Object, Object> uploaders = data.getUploaders();
        Object obj = uploaders.get(location);
        if (obj == null) {
            throw new TException("File for that location does not exist (or timed out)");
        }
        try {
            if (obj instanceof WritableByteChannel) {
                WritableByteChannel channel = (WritableByteChannel) obj;
                channel.close();
                uploaders.remove(location);
                LOG.info("Finished uploading file from client: " + location);
            } else {
                throw new TException("Object isn't WritableByteChannel for " + location);
            }
        } catch (IOException e) {
            LOG.error(" WritableByteChannel close failed when finishFileUpload " + location);
        }

    }

    @Override
    public String beginFileDownload(String file) throws TException {
        BufferFileInputStream is;
        String id;
        try {
            int bufferSize = JStormUtils.parseInt(conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE), 1024 * 1024) / 2;

            is = new BufferFileInputStream(file, bufferSize);
            id = UUID.randomUUID().toString();
            data.getDownloaders().put(id, is);
        } catch (FileNotFoundException e) {
            LOG.error(e + "file:" + file + " not found");
            throw new TException(e);
        }

        return id;
    }

    @Override
    public ByteBuffer downloadChunk(String id) throws TException {
        TimeCacheMap<Object, Object> downloaders = data.getDownloaders();
        Object obj = downloaders.get(id);
        if (obj == null) {
            throw new TException("Could not find input stream for that id");
        }

        try {
            if (obj instanceof BufferFileInputStream) {

                BufferFileInputStream is = (BufferFileInputStream) obj;
                byte[] ret = is.read();
                if (ret != null) {
                    downloaders.put(id, is);
                    return ByteBuffer.wrap(ret);
                }
            } else {
                throw new TException("Object isn't BufferFileInputStream for " + id);
            }
        } catch (IOException e) {
            LOG.error("BufferFileInputStream read failed when downloadChunk ", e);
            throw new TException(e);
        }
        byte[] empty = {};
        return ByteBuffer.wrap(empty);
    }

    @Override
    public void finishFileDownload(String id) throws TException {
        data.getDownloaders().remove(id);
    }


    @Override
    public String beginCreateBlob(String blobKey, SettableBlobMeta meta) throws KeyAlreadyExistsException {
        String sessionId = UUID.randomUUID().toString();
        AtomicOutputStream out = data.getBlobStore().createBlob(blobKey, meta);
        data.getBlobUploaders().put(sessionId, out);
        LOG.info("Created blob for {} with session id {}", blobKey, sessionId);
        return sessionId;
    }

    @Override
    public String beginUpdateBlob(String blobKey) throws KeyNotFoundException {
        String sessionId = UUID.randomUUID().toString();
        AtomicOutputStream out = data.getBlobStore().updateBlob(blobKey);
        data.getBlobUploaders().put(sessionId, out);
        LOG.info("Created upload session for {} with id {}", blobKey, sessionId);
        return sessionId;
    }

    @Override
    public void uploadBlobChunk(String session, ByteBuffer chunk) throws TException {
        AtomicOutputStream os = (AtomicOutputStream) data.getBlobUploaders().get(session);
        if (os == null) {
            throw new TException("Blob for session " + session + " does not exist (or timed out)");
        }
        byte[] chunkArray = chunk.array();
        int remaining = chunk.remaining();
        int arrayOffset = chunk.arrayOffset();
        int position = chunk.position();
        try {
            os.write(chunkArray, (arrayOffset + position), remaining);
            data.getBlobUploaders().put(session, os);
        } catch (IOException e) {
            LOG.error("Blob upload failed", e);
            throw new TException(e);
        }
    }

    @Override
    public void finishBlobUpload(String session) throws TException {
        AtomicOutputStream os = (AtomicOutputStream) data.getBlobUploaders().get(session);
        if (os == null) {
            throw new TException("Blob for session " + session + " does not exist (or timed out)");
        }
        try {
            os.close();
        } catch (IOException e) {
            LOG.error("AtomicOutputStream close failed when finishBlobUpload for session {}", session);

        }
        LOG.info("Finished uploading blob for session {}. Closing session.", session);
        data.getBlobUploaders().remove(session);
    }

    @Override
    public void cancelBlobUpload(String session) throws TException {
        AtomicOutputStream os = (AtomicOutputStream) data.getBlobUploaders().get(session);
        if (os == null) {
            throw new TException("Blob for session " + session + " does not exist (or timed out)");
        }
        try {
            os.cancel();
        } catch (IOException e) {
            LOG.error("AtomicOutputStream cancel failed when finishBlobUpload for session {}", session);

        }
        LOG.info("Canceling uploading blob for session {}. Closing session.", session);
        data.getBlobUploaders().remove(session);
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws KeyNotFoundException {
        return data.getBlobStore().getBlobMeta(key);
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta) throws KeyNotFoundException {
        data.getBlobStore().setBlobMeta(key, meta);
    }

    @Override
    public BeginDownloadResult beginBlobDownload(String key) throws TException {
        InputStreamWithMeta is = data.getBlobStore().getBlob(key);
        String sessionId = UUID.randomUUID().toString();
        BeginDownloadResult result = null;
        try {
            result = new BeginDownloadResult(is.getVersion(), sessionId);
            result.set_data_size(is.getFileLength());
            int bufferSize = JStormUtils.parseInt(data.getConf().get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), 65536);
            BufferInputStream bufferedIS = new BufferInputStream(is, bufferSize);
            data.getBlobDownloaders().put(sessionId, bufferedIS);
        } catch (IOException e) {
            LOG.error("beginBlobDownload error", e);
            throw new TException(e);
        }
        return result;
    }

    @Override
    public ByteBuffer downloadBlobChunk(String session) throws TException {
        BufferInputStream is = (BufferInputStream) data.getBlobDownloaders().get(session);
        if (is == null) {
            throw new TException("Could not find input stream for session " + session);
        }
        try {
            byte[] ret = is.read();
            data.getBlobDownloaders().put(session, is);
            if (ret.length == 0) {
                is.close();
                data.getBlobDownloaders().remove(session);
            }
            LOG.debug("Sending {} bytes", ret.length);
            return ByteBuffer.wrap(ret);
        } catch (IOException e) {
            LOG.error("BufferInputStream read failed when downloadBlobChunk ", e);
            throw new TException(e);
        }
    }

    @Override
    public void deleteBlob(String key) throws TException {
        BlobStore blobStore = data.getBlobStore();
        blobStore.deleteBlob(key);
        if (blobStore instanceof LocalFsBlobStore) {
            try {
                data.getStormClusterState().remove_blobstore_key(key);
                data.getStormClusterState().remove_key_version(key);
            } catch (Exception e) {
                throw new TException(e);
            }
        }
        LOG.info("Deleted blob for key {}", key);
    }

    @Override
    public ListBlobsResult listBlobs(String session) throws TException {
        Iterator<String> keysIter;
        if (StringUtils.isBlank(session)) {
            keysIter = data.getBlobStore().listKeys();
        } else {
            keysIter = (Iterator) data.getBlobListers().get(session);
        }
        if (keysIter == null) {
            throw new TException("Blob list for session " + session + " not exist (or time out)");
        }

        // Create a new session id if the user gave an empty session string.
        // This is the use case when the user wishes to list blobs
        // starting from the beginning.
        if (StringUtils.isBlank(session)) {
            session = UUID.randomUUID().toString();
            LOG.info("Creating new session for downloading list {}", session);
        }

        if (!keysIter.hasNext()) {
            data.getBlobListers().remove(session);
            LOG.info("No more blobs to list for session {}", session);
            return new ListBlobsResult(new ArrayList<String>(0), session);
        } else {
            List<String> listChunk = Lists.newArrayList(Iterators.limit(keysIter, 100));
            LOG.info("{} downloading {} entries", session, listChunk.size());
            data.getBlobListers().put(session, keysIter);
            return new ListBlobsResult(listChunk, session);
        }
    }

    @Override
    public int getBlobReplication(String key) throws TException {
        try {
            return data.getBlobStore().getBlobReplication(key);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws TException {
        try {
            return data.getBlobStore().updateBlobReplication(key, replication);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public void createStateInZookeeper(String key) throws TException {
        BlobStore blobStore = data.getBlobStore();
        NimbusInfo nimbusInfo = data.getNimbusHostPortInfo();
        if (blobStore instanceof LocalFsBlobStore) {
            int versionForKey = BlobStoreUtils.getVersionForKey(key, nimbusInfo, data.getConf());
            try {
                data.getStormClusterState().setup_blobstore(key, nimbusInfo, versionForKey);
            } catch (Exception e) {
                throw new TException("create state in zookeeper error", e);
            }
        }
        LOG.debug("Created state in zookeeper for key:{} for nimbus:{}", key, nimbusInfo);
    }

    /**
     * get cluster's summary, it will contain SupervisorSummary and TopologySummary
     *
     * @return ClusterSummary
     */
    @Override
    public ClusterSummary getClusterInfo() throws TException {
        long start = System.nanoTime();
        try {
            StormClusterState stormClusterState = data.getStormClusterState();

            Map<String, Assignment> assignments = new HashMap<String, Assignment>();

            // get TopologySummary
            List<TopologySummary> topologySummaries = NimbusUtils.getTopologySummary(stormClusterState, assignments);

            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster.get_all_SupervisorInfo(stormClusterState, null);

            // generate SupervisorSummaries
            List<SupervisorSummary> supervisorSummaries = NimbusUtils.mkSupervisorSummaries(supervisorInfos, assignments);

            NimbusSummary nimbusSummary = NimbusUtils.getNimbusSummary(stormClusterState, supervisorSummaries, data);

            return new ClusterSummary(nimbusSummary, supervisorSummaries, topologySummaries);
        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getClusterInfo", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    @Override
    public String getVersion() throws TException {
        return Utils.getVersion();
    }

    @Override
    public SupervisorWorkers getSupervisorWorkers(String host) throws NotAliveException, TException {
        return getSupervisorWorkersByHostOrId(host, null);
    }

    @Override
    public SupervisorWorkers getSupervisorWorkersById(String id) throws NotAliveException, TException {
        return getSupervisorWorkersByHostOrId(null, id);
    }

    /**
     * gets supervisor workers by host or supervisor id, note that id priors to host.
     *
     * @param host host
     * @param id   supervisor id
     * @return supervisor workers
     */
    private SupervisorWorkers getSupervisorWorkersByHostOrId(String host, String id) throws TException {
        long start = System.nanoTime();

        if (StringUtils.isBlank(id) && StringUtils.isBlank(host)) {
            throw new TException("Must specify host or supervisor id!");
        }

        try {
            StormClusterState stormClusterState = data.getStormClusterState();

            // all supervisors
            Map<String, SupervisorInfo> supervisorInfos = Cluster.get_all_SupervisorInfo(stormClusterState, null);
            SupervisorInfo supervisorInfo = null;
            String ip;
            if (!StringUtils.isBlank(id)) {
                supervisorInfo = supervisorInfos.get(id);
                host = supervisorInfo.getHostName();
                ip = NetWorkUtils.host2Ip(host);
            } else {
                ip = NetWorkUtils.host2Ip(host);
                for (Entry<String, SupervisorInfo> entry : supervisorInfos.entrySet()) {
                    SupervisorInfo info = entry.getValue();
                    if (info.getHostName().equals(host) || info.getHostName().equals(ip)) {
                        id = entry.getKey();
                        supervisorInfo = info;
                        break;
                    }
                }
            }
            if (supervisorInfo == null) {
                throw new TException("unknown supervisor id:" + id);
            }

            Map<String, Assignment> assignments = Cluster.get_all_assignment(stormClusterState, null);
            Map<Integer, WorkerSummary> portWorkerSummarys = new TreeMap<Integer, WorkerSummary>();
            int usedSlotNumber = 0;
            Map<String, Map<Integer, String>> topologyTaskToComponent = new HashMap<String, Map<Integer, String>>();

            Map<String, MetricInfo> metricInfoMap = new HashMap<String, MetricInfo>();
            for (Entry<String, Assignment> entry : assignments.entrySet()) {
                String topologyId = entry.getKey();
                Assignment assignment = entry.getValue();

                Set<ResourceWorkerSlot> workers = assignment.getWorkers();
                for (ResourceWorkerSlot worker : workers) {
                    if (!id.equals(worker.getNodeId())) {
                        continue;
                    }
                    usedSlotNumber++;

                    Integer port = worker.getPort();
                    WorkerSummary workerSummary = portWorkerSummarys.get(port);
                    if (workerSummary == null) {
                        workerSummary = new WorkerSummary();
                        workerSummary.set_port(port);
                        workerSummary.set_topology(topologyId);
                        workerSummary.set_tasks(new ArrayList<TaskComponent>());

                        portWorkerSummarys.put(port, workerSummary);
                    }

                    Map<Integer, String> taskToComponent = topologyTaskToComponent.get(topologyId);
                    if (taskToComponent == null) {
                        taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, null);
                        topologyTaskToComponent.put(topologyId, taskToComponent);
                    }

                    int earliest = TimeUtils.current_time_secs();
                    for (Integer taskId : worker.getTasks()) {
                        TaskComponent taskComponent = new TaskComponent();
                        taskComponent.set_component(taskToComponent.get(taskId));
                        taskComponent.set_taskId(taskId);
                        Integer startTime = assignment.getTaskStartTimeSecs().get(taskId);
                        if (startTime != null && startTime < earliest) {
                            earliest = startTime;
                        }

                        workerSummary.add_to_tasks(taskComponent);
                    }

                    workerSummary.set_uptime(TimeUtils.time_delta(earliest));

                    String workerSlotName = getWorkerSlotName(ip, port);
                    List<MetricInfo> workerMetricInfoList = this.data.getMetricCache().getMetricData(topologyId, MetaType.WORKER);
                    if (workerMetricInfoList.size() > 0) {
                        MetricInfo workerMetricInfo = workerMetricInfoList.get(0);
                        // remove metrics that don't belong to current worker
                        for (Iterator<String> itr = workerMetricInfo.get_metrics().keySet().iterator();
                             itr.hasNext(); ) {
                            String metricName = itr.next();
                            if (!metricName.contains(ip)) {
                                itr.remove();
                            }
                        }
                        metricInfoMap.put(workerSlotName, workerMetricInfo);
                    }
                }
            }

            List<WorkerSummary> workerList = new ArrayList<WorkerSummary>();
            workerList.addAll(portWorkerSummarys.values());

            Map<String, Integer> supervisorToUsedSlotNum = new HashMap<String, Integer>();
            supervisorToUsedSlotNum.put(id, usedSlotNumber);
            SupervisorSummary supervisorSummary = NimbusUtils.mkSupervisorSummary(supervisorInfo, id, supervisorToUsedSlotNum);

            return new SupervisorWorkers(supervisorSummary, workerList, metricInfoMap);
        } catch (TException e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get ClusterSummary ", e);
            throw new TException(e);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getSupervisorWorkers", (end - start) / TimeUtils.NS_PER_US);
        }

    }

    private String getWorkerSlotName(String hostname, Integer port) {
        return hostname + ":" + port;
    }

    /**
     * Get TopologyInfo, it contain all data of the topology running status
     *
     * @return TopologyInfo
     */
    @Override
    public TopologyInfo getTopologyInfo(String topologyId) throws NotAliveException, TException {
        long start = System.nanoTime();
        StormClusterState stormClusterState = data.getStormClusterState();

        try {

            // get topology's StormBase
            StormBase base = stormClusterState.storm_base(topologyId, null);
            if (base == null) {
                throw new NotAliveException("No topology of " + topologyId);
            }

            Assignment assignment = stormClusterState.assignment_info(topologyId, null);
            if (assignment == null) {
                throw new NotAliveException("No topology of " + topologyId);
            }

            TopologyTaskHbInfo topologyTaskHbInfo = data.getTasksHeartbeat().get(topologyId);
            Map<Integer, TaskHeartbeat> taskHbMap = null;
            if (topologyTaskHbInfo != null)
                taskHbMap = topologyTaskHbInfo.get_taskHbs();

            Map<Integer, TaskInfo> taskInfoMap = Cluster.get_all_taskInfo(stormClusterState, topologyId);
            Map<Integer, String> taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, taskInfoMap);
            Map<Integer, String> taskToType = Cluster.get_all_task_type(stormClusterState, topologyId, taskInfoMap);


            String errorString;
            if (Cluster.is_topology_exist_error(stormClusterState, topologyId)) {
                errorString = "Y";
            } else {
                errorString = "";
            }

            TopologySummary topologySummary = new TopologySummary();
            topologySummary.set_id(topologyId);
            topologySummary.set_name(base.getStormName());
            topologySummary.set_uptimeSecs(TimeUtils.time_delta(base.getLanchTimeSecs()));
            topologySummary.set_status(base.getStatusString());
            topologySummary.set_numTasks(NimbusUtils.getTopologyTaskNum(assignment));
            topologySummary.set_numWorkers(assignment.getWorkers().size());
            topologySummary.set_errorInfo(errorString);

            Map<String, ComponentSummary> componentSummaryMap = new HashMap<String, ComponentSummary>();
            HashMap<String, List<Integer>> componentToTasks = JStormUtils.reverse_map(taskToComponent);
            for (Entry<String, List<Integer>> entry : componentToTasks.entrySet()) {
                String name = entry.getKey();
                List<Integer> taskIds = entry.getValue();
                if (taskIds == null || taskIds.size() == 0) {
                    LOG.warn("No task of component " + name);
                    continue;
                }

                ComponentSummary componentSummary = new ComponentSummary();
                componentSummaryMap.put(name, componentSummary);

                componentSummary.set_name(name);
                componentSummary.set_type(taskToType.get(taskIds.get(0)));
                componentSummary.set_parallel(taskIds.size());
                componentSummary.set_taskIds(taskIds);
            }

            Map<Integer, TaskSummary> taskSummaryMap = new TreeMap<Integer, TaskSummary>();
            Map<Integer, List<TaskError>> taskErrors = Cluster.get_all_task_errors(stormClusterState, topologyId);

            for (Integer taskId : taskInfoMap.keySet()) {
                TaskSummary taskSummary = new TaskSummary();
                taskSummaryMap.put(taskId, taskSummary);

                taskSummary.set_taskId(taskId);
                if (taskHbMap == null) {
                    taskSummary.set_status("Starting");
                    taskSummary.set_uptime(0);
                } else {
                    TaskHeartbeat hb = taskHbMap.get(taskId);
                    if (hb == null) {
                        taskSummary.set_status("Starting");
                        taskSummary.set_uptime(0);
                    } else {
                        boolean isInactive = NimbusUtils.isTaskDead(data, topologyId, taskId);
                        if (isInactive)
                            taskSummary.set_status("INACTIVE");
                        else
                            taskSummary.set_status("ACTIVE");
                        taskSummary.set_uptime(hb.get_uptime());
                    }
                }

                if (StringUtils.isBlank(errorString)) {
                    continue;
                }

                List<TaskError> taskErrorList = taskErrors.get(taskId);
                if (taskErrorList != null && taskErrorList.size() != 0) {
                    for (TaskError taskError : taskErrorList) {
                        ErrorInfo errorInfo = new ErrorInfo(taskError.getError(), taskError.getTimSecs(),
                                taskError.getLevel(), taskError.getCode());
                        taskSummary.add_to_errors(errorInfo);
                        String component = taskToComponent.get(taskId);
                        componentSummaryMap.get(component).add_to_errors(errorInfo);
                    }
                }
            }

            for (ResourceWorkerSlot workerSlot : assignment.getWorkers()) {
                String hostname = workerSlot.getHostname();
                int port = workerSlot.getPort();

                for (Integer taskId : workerSlot.getTasks()) {
                    TaskSummary taskSummary = taskSummaryMap.get(taskId);
                    taskSummary.set_host(hostname);
                    taskSummary.set_port(port);
                }
            }

            TopologyInfo topologyInfo = new TopologyInfo();
            topologyInfo.set_topology(topologySummary);
            topologyInfo.set_components(JStormUtils.mk_list(componentSummaryMap.values()));
            topologyInfo.set_tasks(JStormUtils.mk_list(taskSummaryMap.values()));

            // return topology metric & component metric only
            List<MetricInfo> tpMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.TOPOLOGY);
            List<MetricInfo> compMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.COMPONENT);
            List<MetricInfo> workerMetricList = data.getMetricCache().getMetricData(topologyId, MetaType.WORKER);
            MetricInfo taskMetric = MetricUtils.mkMetricInfo();
            MetricInfo streamMetric = MetricUtils.mkMetricInfo();
            MetricInfo nettyMetric = MetricUtils.mkMetricInfo();
            MetricInfo tpMetric, compMetric, workerMetric;

            if (tpMetricList == null || tpMetricList.size() == 0) {
                tpMetric = MetricUtils.mkMetricInfo();
            } else {
                // get the last min topology metric
                tpMetric = tpMetricList.get(tpMetricList.size() - 1);
            }
            if (compMetricList == null || compMetricList.size() == 0) {
                compMetric = MetricUtils.mkMetricInfo();
            } else {
                compMetric = compMetricList.get(0);
            }
            if (workerMetricList == null || workerMetricList.size() == 0) {
                workerMetric = MetricUtils.mkMetricInfo();
            } else {
                workerMetric = workerMetricList.get(0);
            }
            TopologyMetric topologyMetrics = new TopologyMetric(tpMetric, compMetric, workerMetric,
                    taskMetric, streamMetric, nettyMetric);
            topologyInfo.set_metrics(topologyMetrics);

            return topologyInfo;
        } catch (TException e) {
            LOG.info("Failed to get topologyInfo " + topologyId, e);
            throw e;
        } catch (Exception e) {
            LOG.info("Failed to get topologyInfo " + topologyId, e);
            throw new TException("Failed to get topologyInfo" + topologyId);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyInfo", (end - start) / TimeUtils.NS_PER_US);
        }

    }

    @Override
    public TopologyInfo getTopologyInfoByName(String topologyName) throws NotAliveException, TException {
        String topologyId = getTopologyId(topologyName);
        return getTopologyInfo(topologyId);

    }

    @Override
    public Map<Integer, String> getTopologyTasksToSupervisorIds(String topologyName) throws NotAliveException, TException {
        StormClusterState stormClusterState = data.getStormClusterState();
        String topologyId = getTopologyId(topologyName);
        Map<Integer, String> ret = new HashMap<>();
        try {
            Assignment assignment = stormClusterState.assignment_info(topologyId, null);
            Set<ResourceWorkerSlot> workers = assignment.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String supervisorId = worker.getNodeId();
                for (Integer task : worker.getTasks()) {
                    ret.put(task, supervisorId);
                }
            }
        } catch (Exception ex) {
            LOG.error("Error:", ex);
        }
        return ret;
    }

    @Override
    public String getNimbusConf() throws TException {
        try {
            return JStormUtils.to_json(data.getConf());
        } catch (Exception e) {
            String err = "Failed to generate Nimbus configuration";
            LOG.error(err, e);
            throw new TException(err);
        }
    }

    @Override
    public String getStormRawConf() throws TException {
        try {
            return FileUtils.readFileToString(new File(LoadConf.getStormYamlPath()));
        } catch (IOException ex) {
            throw new TException(ex);
        }
    }

    @Override
    public String getSupervisorConf(String id) throws TException {
        Map<Object, Object> ret = new HashMap<>();
        ret.putAll(data.getConf());

        try {
            SupervisorInfo supervisorInfo =
                    (SupervisorInfo) (((StormZkClusterState) this.data.getStormClusterState()).getObject(Cluster.supervisor_path(id), false));

            if (supervisorInfo != null && supervisorInfo.getSupervisorConf() != null) {
                ret.putAll(supervisorInfo.getSupervisorConf());
            } else {
                LOG.warn("supervisor conf not found in nimbus cache, supervisor id:{}, so fall back to nimbus conf", id);
            }
        } catch (Exception ex) {
            LOG.error("Error:", ex);
        }
        return JStormUtils.to_json(ret);
    }

    /**
     * get topology configuration
     *
     * @param id String: topology id
     * @return String
     */
    @Override
    public String getTopologyConf(String id) throws NotAliveException, TException {
        String rtn;
        try {
            Map<Object, Object> topologyConf = StormConfig.read_nimbus_topology_conf(id, data.getBlobStore());
            rtn = JStormUtils.to_json(topologyConf);
        } catch (IOException e) {
            LOG.info("Failed to get configuration of " + id, e);
            throw new TException(e);
        }
        return rtn;
    }

    @Override
    public String getTopologyId(String topologyName) throws NotAliveException, TException {
        StormClusterState stormClusterState = data.getStormClusterState();

        try {
            // get all active topology's StormBase
            String topologyId = Cluster.get_topology_id(stormClusterState, topologyName);
            if (topologyId != null) {
                return topologyId;
            }

        } catch (Exception e) {
            LOG.info("Failed to get getTopologyId " + topologyName, e);
            throw new TException("Failed to get getTopologyId " + topologyName);
        }

        // topologyId == null
        throw new NotAliveException("No topology of " + topologyName);
    }

    /**
     * get StormTopology throw deserialize local files
     *
     * @param id String: topology id
     * @return StormTopology
     */
    @Override
    public StormTopology getTopology(String id) throws NotAliveException, TException {
        StormTopology topology;
        try {
            StormTopology stormtopology = StormConfig.read_nimbus_topology_code(id, data.getBlobStore());
            if (stormtopology == null) {
                throw new NotAliveException("No topology of " + id);
            }

            Map<Object, Object> topologyConf = (Map<Object, Object>) StormConfig.read_nimbus_topology_conf(id, data.getBlobStore());

            topology = Common.system_topology(topologyConf, stormtopology);
        } catch (Exception e) {
            LOG.error("Failed to get topology " + id + ",", e);
            throw new TException("Failed to get system_topology");
        }
        return topology;
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, TException {
        StormTopology topology = null;
        try {
            StormTopology stormtopology = StormConfig.read_nimbus_topology_code(id, data.getBlobStore());
            if (stormtopology == null) {
                throw new NotAliveException("No topology of " + id);
            }

            return topology;
        } catch (Exception e) {
            LOG.error("Failed to get topology " + id + ",", e);
            throw new TException("Failed to get system_topology");
        }

    }

    /**
     * check whether the topology is bActive?
     *
     * @param nimbus
     * @param topologyName
     * @param bActive
     * @throws Exception
     */
    public void checkTopologyActive(NimbusData nimbus, String topologyName, boolean bActive) throws Exception {
        if (isTopologyActive(nimbus.getStormClusterState(), topologyName) != bActive) {
            if (bActive) {
                throw new NotAliveException(topologyName + " is not alive");
            } else {
                throw new AlreadyAliveException(topologyName + " is already active");
            }
        }
    }

    /**
     * whether the topology is active by topology name
     *
     * @param stormClusterState see Cluster_clj
     * @param topologyName
     * @return boolean if the storm is active, return true, otherwise return
     * false;
     * @throws Exception
     */
    public boolean isTopologyActive(StormClusterState stormClusterState, String topologyName) throws Exception {
        boolean rtn = false;
        if (Cluster.get_topology_id(stormClusterState, topologyName) != null) {
            rtn = true;
        }
        return rtn;
    }

    /**
     * create local topology files in blobstore and sync metadata to zk
     */
    private void setupStormCode(Map<Object, Object> conf, String topologyId, String tmpJarLocation, Map<Object, Object> stormConf, StormTopology topology)
            throws Exception {
        StormClusterState clusterState = data.getStormClusterState();
        BlobStore blobStore = data.getBlobStore();
        NimbusInfo nimbusInfo = data.getNimbusHostPortInfo();
        String codeKey = StormConfig.master_stormcode_key(topologyId);
        String confKey = StormConfig.master_stormconf_key(topologyId);

        // in local mode there is no jar
        if (tmpJarLocation != null) {
            setupJar(tmpJarLocation, topologyId, blobStore, clusterState, nimbusInfo, false);
        }
        blobStore.createBlob(confKey, Utils.serialize(stormConf), new SettableBlobMeta());
        blobStore.createBlob(codeKey, Utils.serialize(topology), new SettableBlobMeta());
        if (blobStore instanceof LocalFsBlobStore) {
            clusterState.setup_blobstore(confKey, nimbusInfo, BlobStoreUtils.getVersionForKey(confKey, nimbusInfo, conf));
            clusterState.setup_blobstore(codeKey, nimbusInfo, BlobStoreUtils.getVersionForKey(codeKey, nimbusInfo, conf));
        }

        LOG.info("Successfully create blobstore for topology {}", topologyId);
    }

    public void setupJar(String tmpJarLocation, String topologyId, BlobStore blobStore,
                         StormClusterState clusterState, NimbusInfo nimbusInfo, boolean update) throws Exception {
        // setup lib files
        boolean existLibs = true;
        String srcLibPath = StormConfig.stormlib_path(tmpJarLocation);
        File srcFile = new File(srcLibPath);
        if (srcFile.exists()) {
            File[] libJars = srcFile.listFiles();
            if (libJars != null) {
                for (File jar : libJars) {
                    if (jar.isFile()) {
                        String libJarKey = StormConfig.master_stormlib_key(topologyId, jar.getName());
                        if (update) {
                            BlobStoreUtils.updateBlob(blobStore, libJarKey, new FileInputStream(jar));
                        } else {
                            blobStore.createBlob(libJarKey, new FileInputStream(jar), new SettableBlobMeta());
                        }
                        if (blobStore instanceof LocalFsBlobStore) {
                            clusterState.setup_blobstore(libJarKey, nimbusInfo, BlobStoreUtils.getVersionForKey(libJarKey, nimbusInfo, conf));
                        }
                    }
                }
            }
        } else {
            LOG.info("No lib jars " + srcLibPath);
            existLibs = false;
        }

        // find storm jar path
        String jarPath = null;
        List<String> files = PathUtils.read_dir_contents(tmpJarLocation);
        for (String file : files) {
            if (file.endsWith(".jar")) {
                jarPath = tmpJarLocation + PathUtils.SEPERATOR + file;
                break;
            }
        }
        if (jarPath == null) {
            if (!existLibs) {
                throw new IllegalArgumentException("No jar under " + tmpJarLocation);
            } else {
                LOG.info("No submit jar");
                return;
            }
        }

        // setup storm jar
        String jarKey = StormConfig.master_stormjar_key(topologyId);
        if (update) {
            BlobStoreUtils.updateBlob(blobStore, jarKey, new FileInputStream(jarPath));
        } else {
            blobStore.createBlob(jarKey, new FileInputStream(jarPath), new SettableBlobMeta());
        }
        if (blobStore instanceof LocalFsBlobStore) {
            int versionInfo = BlobStoreUtils.getVersionForKey(jarKey, nimbusInfo, conf);
            clusterState.setup_blobstore(jarKey, nimbusInfo, versionInfo);
        }

        PathUtils.rmr(tmpJarLocation);
    }

    private void waitForDesiredCodeReplication(Map conf, String topologyId) {
        int minReplicationCount = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_MIN_REPLICATION_COUNT), 1);
        int maxWaitTime = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC), 0);

        try {
            List<String> blobKeys = BlobStoreUtils.getKeyListFromId(data, topologyId);
            Map<String, Integer> blobKeysToReplicationCount = new HashMap<>();
            for (String key : blobKeys) {
                blobKeysToReplicationCount.put(key, 0);
            }
            refreshBlobReplicationCount(blobKeysToReplicationCount, minReplicationCount);
            int totalWaitTime = 0;
            while (isNeedWait(minReplicationCount, maxWaitTime, blobKeysToReplicationCount, totalWaitTime)) {
                Thread.sleep(1);
                LOG.info("waiting for desired replication to be achieved. min-replication-count = {}, " +
                                "max-replication-wait-time = {}, total-wait-time = {}, current key to replication count = {}",
                        minReplicationCount, maxWaitTime, blobKeysToReplicationCount);
                refreshBlobReplicationCount(blobKeysToReplicationCount, minReplicationCount);
                totalWaitTime++;
            }
            boolean isAllAchieved = true;
            for (Integer count : blobKeysToReplicationCount.values()) {
                if (count <= minReplicationCount) {
                    isAllAchieved = false;
                    break;
                }
            }
            if (isAllAchieved) {
                LOG.info("desired replication count {} achieved, current key to replication count = {}",
                        minReplicationCount, blobKeysToReplicationCount);
            } else {
                LOG.info("desired replication count of {} not achieved but we have hit the max wait time {}, " +
                        "so moving on with key to replication count {}", minReplicationCount, maxWaitTime, blobKeysToReplicationCount);
            }
        } catch (Exception e) {
            LOG.error("wait for desired code replication error", e);
        }
    }

    private void refreshBlobReplicationCount(Map<String, Integer> keysCount, int minReplicationCount) throws Exception {
        for (String key : keysCount.keySet()) {
            if (data.isLocalMode() && key.endsWith("-stormjar.jar")) {
                keysCount.put(key, minReplicationCount);
            } else {
                int replicationCount = data.getBlobStore().getBlobReplication(key);
                keysCount.put(key, replicationCount);
            }
        }
    }

    private boolean isNeedWait(int minReplicationCount, int maxWaitTime, Map<String, Integer> keysCount, int totalWaitTime) {
        boolean isNeedWait = false;
        for (Integer count : keysCount.values()) {
            if (count < minReplicationCount) {
                isNeedWait = true;
                break;
            }
        }
        if (isNeedWait) {
            return maxWaitTime < 0 || totalWaitTime < maxWaitTime;
        } else {
            return false;
        }
    }

    /**
     * generate TaskInfo for every bolt or spout in ZK /ZK/tasks/topoologyId/xxx
     *
     * @param conf
     * @param topologyId
     * @param stormClusterState
     * @throws Exception
     */
    public void setupZkTaskInfo(Map<Object, Object> conf, String topologyId, StormClusterState stormClusterState) throws Exception {
        Map<Integer, TaskInfo> taskToTaskInfo = mkTaskComponentAssignments(conf, topologyId);

        // mkdir /ZK/taskbeats/topoologyId
        int masterId = NimbusUtils.getTopologyMasterId(taskToTaskInfo);
        TopologyTaskHbInfo topoTaskHbinfo = new TopologyTaskHbInfo(topologyId, masterId);
        data.getTasksHeartbeat().put(topologyId, topoTaskHbinfo);
        stormClusterState.topology_heartbeat(topologyId, topoTaskHbinfo);

        if (taskToTaskInfo == null || taskToTaskInfo.size() == 0) {
            throw new InvalidTopologyException("Failed to generate TaskIDs map");
        }
        // key is taskid, value is taskinfo
        stormClusterState.set_task(topologyId, taskToTaskInfo);
    }

    /**
     * generate a taskid(Integer) for every task
     *
     * @param conf
     * @param topologyid
     * @return Map<Integer, String>: from taskid to componentid
     * @throws IOException
     * @throws InvalidTopologyException
     */
    public Map<Integer, TaskInfo> mkTaskComponentAssignments(Map<Object, Object> conf, String topologyid)
            throws IOException, InvalidTopologyException, KeyNotFoundException {

        // @@@ here exist a little problem,
        // we can directly pass stormConf from Submit method
        Map<Object, Object> stormConf = StormConfig.read_nimbus_topology_conf(topologyid, data.getBlobStore());
        StormTopology stopology = StormConfig.read_nimbus_topology_code(topologyid, data.getBlobStore());
        StormTopology topology = Common.system_topology(stormConf, stopology);

        return Common.mkTaskInfo(stormConf, topology, topologyid);
    }

    @Override
    public void metricMonitor(String topologyName, MonitorOptions options) throws TException {
        boolean isEnable = options.is_isEnable();
        StormClusterState clusterState = data.getStormClusterState();

        try {
            String topologyId = Cluster.get_topology_id(clusterState, topologyName);
            if (null != topologyId) {
                clusterState.set_storm_monitor(topologyId, isEnable);
            } else {
                throw new NotAliveException("Failed to update metricsMonitor status as " + topologyName + " is not alive");
            }
        } catch (Exception e) {
            String errMsg = "Failed to update metricsMonitor " + topologyName;
            LOG.error(errMsg, e);
            throw new TException(e);
        }

    }

    @Override
    public TopologyMetric getTopologyMetrics(String topologyId) throws TException {
        LOG.debug("Nimbus service handler, getTopologyMetric, topology ID: " + topologyId);
        long start = System.nanoTime();
        try {
            return ClusterMetricsRunnable.getInstance().getContext().getTopologyMetric(topologyId);
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyMetric", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    @Override
    public void uploadTopologyMetrics(String topologyId, TopologyMetric uploadMetrics) throws TException {
        LOG.debug("Received topology metrics:{}", topologyId);

        UpdateEvent.pushEvent(topologyId, uploadMetrics);
    }

    @Override
    public Map<String, Long> registerMetrics(String topologyId, Set<String> metrics) throws TException {
        try {
            return ClusterMetricsRunnable.getInstance().getContext().registerMetrics(topologyId, metrics);
        } catch (Exception ex) {
            LOG.error("registerMetrics error", ex);
            throw new TException(ex);
        }
    }

    public void uploadNewCredentials(String topologyName, Credentials creds) {
    }

    @Override
    public List<MetricInfo> getMetrics(String topologyId, int type) throws TException {
        MetaType metaType = MetaType.parse(type);
        return data.getMetricCache().getMetricData(topologyId, metaType);
    }

    @Override
    public MetricInfo getNettyMetrics(String topologyId) throws TException {
        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            return metricInfoList.get(0);
        }
        return new MetricInfo();
    }

    @Override
    public MetricInfo getNettyMetricsByHost(String topologyId, String host) throws TException {
        MetricInfo ret = new MetricInfo();

        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            MetricInfo metricInfo = metricInfoList.get(0);
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metricInfo.get_metrics().entrySet()) {
                String metricName = metricEntry.getKey();
                Map<Integer, MetricSnapshot> data = metricEntry.getValue();
                if (metricName.contains(host)) {
                    ret.put_to_metrics(metricName, data);
                }
            }
        }

        LOG.info("getNettyMetricsByHost, total size:{}", ret.get_metrics_size());
        return ret;
    }

    @Override
    public int getNettyMetricSizeByHost(String topologyId, String host) throws TException {
        return getNettyMetricsByHost(topologyId, host).get_metrics_size();
    }

    @Override
    public MetricInfo getPagingNettyMetrics(String topologyId, String host, int page) throws TException {
        MetricInfo ret = new MetricInfo();

        int start = (page - 1) * MetricUtils.NETTY_METRIC_PAGE_SIZE;
        int end = page * MetricUtils.NETTY_METRIC_PAGE_SIZE;
        int cur = -1;
        List<MetricInfo> metricInfoList = data.getMetricCache().getMetricData(topologyId, MetaType.NETTY);
        if (metricInfoList != null && metricInfoList.size() > 0) {
            MetricInfo metricInfo = metricInfoList.get(0);
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metricInfo.get_metrics().entrySet()) {
                String metricName = metricEntry.getKey();
                Map<Integer, MetricSnapshot> data = metricEntry.getValue();
                if (metricName.contains(host)) {
                    ++cur;
                    if (cur >= start && cur < end) {
                        ret.put_to_metrics(metricName, data);
                    }
                    if (cur >= end) {
                        break;
                    }
                }
            }
        }

        LOG.info("getNettyMetricsByHost, total size:{}", ret.get_metrics_size());
        return ret;
    }

    @Override
    public MetricInfo getTaskMetrics(String topologyId, String component) throws TException {
        List<MetricInfo> taskMetricList = getMetrics(topologyId, MetaType.TASK.getT());
        if (taskMetricList != null && taskMetricList.size() > 0) {
            MetricInfo metricInfo = taskMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[2].equals(component)) {
                    itr.remove();
                }
            }
            LOG.info("taskMetric, total size:{}", metricInfo.get_metrics_size());
            return metricInfo;
        }
        return MetricUtils.mkMetricInfo();
    }

    @Override
    public List<MetricInfo> getTaskAndStreamMetrics(String topologyId, int taskId) throws TException {
        List<MetricInfo> taskMetricList = getMetrics(topologyId, MetaType.TASK.getT());
        List<MetricInfo> streamMetricList = getMetrics(topologyId, MetaType.STREAM.getT());

        String taskIdStr = taskId + "";
        MetricInfo taskMetricInfo;
        if (taskMetricList != null && taskMetricList.size() > 0) {
            taskMetricInfo = taskMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = taskMetricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[3].equals(taskIdStr)) {
                    itr.remove();
                }
            }
        } else {
            taskMetricInfo = MetricUtils.mkMetricInfo();
        }

        MetricInfo streamMetricInfo;
        if (streamMetricList != null && streamMetricList.size() > 0) {
            streamMetricInfo = streamMetricList.get(0);
            Map<String, Map<Integer, MetricSnapshot>> metrics = streamMetricInfo.get_metrics();
            for (Iterator<String> itr = metrics.keySet().iterator(); itr.hasNext(); ) {
                String metricName = itr.next();
                String[] parts = metricName.split(MetricUtils.DELIM);
                if (parts.length < 7 || !parts[3].equals(taskIdStr)) {
                    itr.remove();
                }
            }
        } else {
            streamMetricInfo = MetricUtils.mkMetricInfo();
        }
        return Lists.newArrayList(taskMetricInfo, streamMetricInfo);
    }

    @Override
    public List<MetricInfo> getSummarizedTopologyMetrics(String topologyId) throws TException {
        return data.getMetricCache().getMetricData(topologyId, MetaType.TOPOLOGY);
    }

    @Override
    public void updateTopology(String name, String uploadedLocation,
                               String updateConf) throws NotAliveException,
            InvalidTopologyException, TException {
        try {
            //firstly update jar and conf
            checkTopologyActive(data, name, true);
            String topologyId = null;
            StormClusterState stormClusterState = data.getStormClusterState();
            topologyId = Cluster.get_topology_id(stormClusterState, name);
            if (topologyId == null) {
                throw new NotAliveException(name);
            }
            BlobStore blobStore = data.getBlobStore();
            StormClusterState clusterState = data.getStormClusterState();
            NimbusInfo nimbusInfo = data.getNimbusHostPortInfo();
            if (uploadedLocation != null) {
                setupJar(uploadedLocation, topologyId, blobStore, clusterState, nimbusInfo, true);
            }

            Map topoConf = StormConfig.read_nimbus_topology_conf(topologyId, data.getBlobStore());
            Map<Object, Object> config =
                    (Map<Object, Object>) JStormUtils.from_json(updateConf);
            topoConf.putAll(config);

            String confKey = StormConfig.master_stormconf_key(topologyId);

            BlobStoreUtils.updateBlob(blobStore, confKey, Utils.serialize(topoConf));
            if (blobStore instanceof LocalFsBlobStore) {
                clusterState.setup_blobstore(confKey, nimbusInfo, BlobStoreUtils.getVersionForKey(confKey, nimbusInfo, conf));
            }
            NimbusUtils.transitionName(data, name, true,
                    StatusType.update_topology, config);
            LOG.info("update topology " + name + " successfully");
            notifyTopologyActionListener(name, "updateTopology");

        } catch (NotAliveException e) {
            String errMsg = "Error, no this topology " + name;
            LOG.error(errMsg, e);
            throw new NotAliveException(errMsg);
        } catch (Exception e) {
            String errMsg = "Failed to update topology " + name;
            LOG.error(errMsg, e);
            throw new TException(errMsg);
        }

    }

    @Override
    public void updateTaskHeartbeat(TopologyTaskHbInfo taskHbs) throws TException {
        String topologyId = taskHbs.get_topologyId();
        Integer topologyMasterId = taskHbs.get_topologyMasterId();
        TopologyTaskHbInfo nimbusTaskHbs = data.getTasksHeartbeat().get(topologyId);

        if (nimbusTaskHbs == null) {
            nimbusTaskHbs = new TopologyTaskHbInfo(topologyId, topologyMasterId);
            data.getTasksHeartbeat().put(topologyId, nimbusTaskHbs);
        }

        Map<Integer, TaskHeartbeat> nimbusTaskHbMap = nimbusTaskHbs.get_taskHbs();
        if (nimbusTaskHbMap == null) {
            nimbusTaskHbMap = new ConcurrentHashMap<Integer, TaskHeartbeat>();
            nimbusTaskHbs.set_taskHbs(nimbusTaskHbMap);
        }

        Map<Integer, TaskHeartbeat> taskHbMap = taskHbs.get_taskHbs();
        if (taskHbMap != null) {
            for (Entry<Integer, TaskHeartbeat> entry : taskHbMap.entrySet()) {
                nimbusTaskHbMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void setHostInBlackList(String host) throws TException {
        StormClusterState stormClusterState = data.getStormClusterState();
        try {
            stormClusterState.set_in_blacklist(host);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeHostOutBlackList(String host) throws TException {
        StormClusterState stormClusterState = data.getStormClusterState();
        try {
            stormClusterState.remove_from_blacklist(host);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void notifyTopologyActionListener(String topologyName, String action) {
        ITopologyActionNotifierPlugin nimbusNotify = data.getNimbusNotify();
        if (nimbusNotify != null)
            nimbusNotify.notify(topologyName, action);
    }

    @Override
    public void notifyThisTopologyTasksIsDead(String topologyId) throws TException {
        LOG.info("notifyThisTopologyTasksIsDead {}", topologyId);
        ConcurrentHashMap<String, Semaphore> topologyIdtoSem = data.getTopologyIdtoSem();
        Semaphore semaphore = topologyIdtoSem.get(topologyId);
        if (semaphore != null){
            semaphore.release();
        }
    }
}
