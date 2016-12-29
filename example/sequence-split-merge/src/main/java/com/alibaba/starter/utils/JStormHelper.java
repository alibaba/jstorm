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
package com.alibaba.starter.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.cluster.Cluster;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.task.error.TaskError;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.NimbusClientWrapper;
import backtype.storm.utils.Utils;

public final class JStormHelper {
    private final static Logger LOG = LoggerFactory.getLogger(JStormHelper.class);
    
    private static final int MILLIS_IN_SEC = 1000;
    
    private static NimbusClient client = null;
    
    private JStormHelper() {
    }
    
    public static NimbusClient getNimbusClient(Map conf) {
        if (client != null) {
            return client;
        }
        
        if (conf == null) {
            conf = Utils.readStormConfig();
            
        }
        client = NimbusClient.getConfiguredClient(conf);
        return client;
    }
    
    public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf,
            int runtimeInSeconds, Callback callback) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        
        if (runtimeInSeconds < 120) {
            JStormUtils.sleepMs(120 * 1000);
        }else {
            JStormUtils.sleepMs(runtimeInSeconds * 1000);
        }
        
        if (callback != null) {
            callback.execute(topologyName);
        }
        
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
    
    public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf,
            int runtimeInSeconds, Callback callback) throws Exception {
        if (conf.get(Config.TOPOLOGY_WORKERS) == null) {
            conf.setNumWorkers(3);
        }
        
        StormSubmitter.submitTopology(topologyName, conf, topology);
        
        if (JStormUtils.parseBoolean(conf.get("RUN_LONG_TIME"), false)) {
            LOG.info(topologyName + " will run long time");
            return ;
        }
        
        if (runtimeInSeconds < 120) {
            JStormUtils.sleepMs(120 * 1000);
        }else {
            JStormUtils.sleepMs(runtimeInSeconds * 1000);
        }
        
        if (callback != null) {
            callback.execute(topologyName);
        }
        
        killTopology(conf, topologyName);
    }
    
    public static void runTopology(StormTopology topology, String topologyName, Config conf, int runtimeInSeconds,
            Callback callback, boolean isLocal) throws Exception {
        if (isLocal) {
            runTopologyLocally(topology, topologyName, conf, runtimeInSeconds, callback);
            
        } else {
            runTopologyRemotely(topology, topologyName, conf, runtimeInSeconds, callback);
        }
    }
    
    public static void cleanCluster() {
        try {
            NimbusClient client = getNimbusClient(null);
            
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            
            List<TopologySummary> topologySummaries = clusterSummary.get_topologies();
            
            KillOptions killOption = new KillOptions();
            killOption.set_wait_secs(1);
            for (TopologySummary topologySummary : topologySummaries) {
                client.getClient().killTopologyWithOpts(topologySummary.get_name(), killOption);
                LOG.info("Successfully kill " + topologySummary.get_name());
            }
        } catch (Exception e) {
            if (client != null) {
                client.close();
                client = null;
                
            }
            
            LOG.error("Failed to kill all topology ", e);
        }
        
        return;
    }
    
    public static void killTopology(Map conf, String topologyName) throws Exception {
        NimbusClientWrapper client = new NimbusClientWrapper();
        try {
            client.init(conf);
            KillOptions killOption = new KillOptions();
            killOption.set_wait_secs(1);
            client.getClient().killTopologyWithOpts(topologyName, killOption);
        }finally {
            client.cleanup();
        }
    }
    
    /**
     * This function maybe not be useful in the future
     * @param conf
     * @return
     */
    @Deprecated
    public static Map getFullConf(Map conf){
        Map realConf = new HashMap();
        boolean isLocal = StormConfig.try_local_mode(conf);
        if (isLocal) {
            realConf.putAll(LocalCluster.getInstance().getLocalClusterMap().getConf());
                    
        }else {
            realConf.putAll( Utils.readStormConfig());
        }
        realConf.putAll(conf);
        
        return realConf;
    }
    
    /**
     * This function maybe not be useful in the future
     * This function bring some hacks to JStorm, this isn't a good way
     * @param conf
     * @return
     */
    @Deprecated
    public static StormZkClusterState mkStormZkClusterState(Map conf) throws Exception {
        Map realConf = getFullConf(conf);
        
        return new StormZkClusterState(realConf);
    }
    
    /**
     * This function maybe not be useful in the future
     * @param conf
     * @return
     */
    @Deprecated
    public static Map<Integer, List<TaskError>> getTaskErrors(String topologyId, Map conf) throws Exception {
        StormZkClusterState clusterState = null;
        try {
            clusterState = mkStormZkClusterState(conf);
            return Cluster.get_all_task_errors(clusterState, topologyId);
        }finally {
            if (clusterState != null) {
                clusterState.disconnect();
            }
        }
        
    }
    
    /**
     * This function maybe not be useful in the future
     * @param conf
     * @return
     */
    @Deprecated
    public static Assignment getAssignment(String topologyId, Map conf) throws Exception {
        StormZkClusterState clusterState = null;
        try {
            clusterState = mkStormZkClusterState(conf);
            return clusterState.assignment_info(topologyId, null);
        }finally {
            if (clusterState != null) {
                clusterState.disconnect();
            }
        }
        
    }
    
    public static List<String> getSupervisorHosts() throws Exception {
        try {
            List<String> hosts = new ArrayList<>();
            
            NimbusClient client = getNimbusClient(null);
            
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();
            
            List<SupervisorSummary> supervisorSummaries = clusterSummary.get_supervisors();
            
            Collections.sort(supervisorSummaries, new Comparator<SupervisorSummary>() {
                
                @Override
                public int compare(SupervisorSummary o1, SupervisorSummary o2) {
                    // TODO Auto-generated method stub
                    int o1Left = o1.get_numWorkers() - o1.get_numUsedWorkers();
                    int o2Left = o2.get_numWorkers() - o2.get_numUsedWorkers();
                    
                    return o1Left - o2Left;
                }
                
            });
            
            for (SupervisorSummary supervisorSummary : supervisorSummaries) {
                hosts.add(supervisorSummary.get_host());
            }
            return hosts;
        } catch (Exception e) {
            if (client != null) {
                client.close();
                client = null;
                
            }
            
            LOG.error("Failed to kill all topology ", e);
            throw new RuntimeException(e);
        }
        
    }
    
    public static void checkError(Map conf, String topologyName) throws Exception{
        NimbusClientWrapper client = new NimbusClientWrapper();
        try {
            client.init(conf);
            
            String topologyId = client.getClient().getTopologyId(topologyName);
            
            Map<Integer, List<TaskError>> errors = getTaskErrors(topologyId, conf);
            
            for ( Entry<Integer, List<TaskError>> entry: errors.entrySet()) {
                Integer taskId = entry.getKey();
                List<TaskError> errorList = entry.getValue();
                for (TaskError error : errorList) {
                    if (ErrorConstants.ERROR.equals(error.getLevel())) {
                        Assert.fail(taskId + " occur error:" + error.getError());
                    }else if (ErrorConstants.FATAL.equals(error.getLevel())) {
                        Assert.fail(taskId + " occur error:" + error.getError());
                    }
                }
            }
        }finally {
            client.cleanup();
        }
    }
    
    public static class CheckAckedFail implements Callback {
        
        private Map conf;
        
        public CheckAckedFail(Map conf) {
            this.conf = conf;
        }
        
        @Override
        public <T> Object execute(T... args) {
            // TODO Auto-generated method stub
            String topologyName = (String) args[0];
            
            try {
                checkError(conf, topologyName);
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            
            
            Map<String, Double> ret = JStormUtils.getMetrics(conf, topologyName, MetaType.TASK, null);
            for (Entry<String, Double> entry : ret.entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();
                
                if (key.indexOf(MetricDef.FAILED_NUM) > 0) {
                    Assert.assertTrue(key + " fail number should == 0", value == 0);
                } else if (key.indexOf(MetricDef.ACKED_NUM) > 0) {
                    if (value == 0.0) {
                        LOG.warn(key + ":" + value);
                    }else {
                        LOG.info(key + ":" + value);
                    }
                    
                } else if (key.indexOf(MetricDef.EMMITTED_NUM) > 0) {
                    if (value == 0.0) {
                        LOG.warn(key + ":" + value);
                    }else {
                        LOG.info(key + ":" + value);
                    }
                }
            }
            
            return ret;
        }
        
    }
    
    public static Map LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            return LoadConf.LoadYaml(arg);
        } else {
            return LoadConf.LoadProperty(arg);
        }
    }
    
    public static Config getConfig(String[] args) {
        Config ret = new Config();
        if (args == null || args.length == 0) {
            return ret;
        }
        
        if (StringUtils.isBlank(args[0])) {
            return ret;
        }
        
        Map conf = JStormHelper.LoadConf(args[0]);
        ret.putAll(conf);
        return ret;
    }
    
    public static boolean localMode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }
        
        return false;
        
    }
    
    public static void main(String[] args) {
    
    }
    
}
