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
package backtype.storm;

import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LocalCluster implements ILocalCluster {

    public static Logger LOG = LoggerFactory.getLogger(LocalCluster.class);

    private LocalClusterMap state;

    protected void setLogger() {
        // the code is for log4j
        // boolean needReset = true;
        // Logger rootLogger = Logger.getRootLogger();
        // if (rootLogger != null) {
        // Enumeration appenders = rootLogger.getAllAppenders();
        // if (appenders.hasMoreElements() == true) {
        // needReset = false;
        // }
        // }
        //
        // if (needReset == true) {
        // BasicConfigurator.configure();
        // rootLogger.setLevel(Level.INFO);
        // }

    }

    // this is easy to debug
    protected static LocalCluster instance = null;

    public static LocalCluster getInstance() {
        return instance;
    }
    public static void setEnv() {
    	// fix in zk occur Address family not supported by protocol family:
        // connect
        System.setProperty("java.net.preferIPv4Stack", "true");
        
        AsyncLoopRunnable.getShutdown().set(false);
    }

    public LocalCluster() {
        synchronized (LocalCluster.class) {
            if (instance != null) {
                throw new RuntimeException("LocalCluster should be single");
            }
            setLogger();
            
            setEnv();

            this.state = LocalUtils.prepareLocalCluster();
            if (this.state == null)
                throw new RuntimeException("prepareLocalCluster error");

            instance = this;
        }
    }

    @Override
    public void submitTopology(String topologyName, Map conf, StormTopology topology) {
        submitTopologyWithOpts(topologyName, conf, topology, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void submitTopologyWithOpts(String topologyName, Map conf, StormTopology topology, SubmitOptions submitOpts) {
        if (!Utils.isValidConf(conf))
            throw new RuntimeException("Topology conf is not json-serializable");
        
        conf.putAll(LocalUtils.getLocalBaseConf());
        conf.putAll(Utils.readCommandLineOpts());
        
        try {
            if (submitOpts == null) {
                state.getNimbus().submitTopology(topologyName, null, Utils.to_json(conf), topology);
            } else {
                state.getNimbus().submitTopologyWithOpts(topologyName, null, Utils.to_json(conf), topology, submitOpts);
            }

        } catch (Exception e) {
            LOG.error("Failed to submit topology " + topologyName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void killTopology(String topologyName) {
        try {
            // kill topology quickly
            KillOptions killOps = new KillOptions();
            killOps.set_wait_secs(0);
            state.getNimbus().killTopologyWithOpts(topologyName, killOps);
        } catch (Exception e) {
            LOG.error("fail to kill Topology " + topologyName, e);
        }
    }

    @Override
    public void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException {
        try {
            state.getNimbus().killTopologyWithOpts(name, options);
        } catch (TException e) {
            LOG.error("fail to kill Topology " + name, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate(String topologyName) {
        try {
            state.getNimbus().activate(topologyName);
        } catch (Exception e) {
            LOG.error("fail to activate " + topologyName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deactivate(String topologyName) {
        try {
            state.getNimbus().deactivate(topologyName);
        } catch (Exception e) {
            LOG.error("fail to deactivate " + topologyName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rebalance(String name, RebalanceOptions options) {
        try {
            state.getNimbus().rebalance(name, options);
        } catch (Exception e) {
            LOG.error("fail to rebalance " + name, e);
            throw new RuntimeException(e);
        }
    }
    protected void cleanEnv() {
    	System.clearProperty(ConfigExtension.TASK_BATCH_TUPLE);
    }

    @Override
    public void shutdown() {
        LOG.info("Being to shutdown");
        // in order to avoid kill topology's command competition
        // it take 10 seconds to remove topology's node
        JStormUtils.sleepMs(10 * 1000);
        this.state.clean();
        cleanEnv();
        instance = null;
        //wait 10 second to exit to make run multiple junit test
        JStormUtils.sleepMs(10 * 1000);
        LOG.info("Successfully shutdown");
    }

    @Override
    public String getTopologyConf(String id) {
        try {
            return state.getNimbus().getTopologyConf(id);
        } catch (Exception e) {
            LOG.error("fail to get topology Conf of topologId: " + id, e);
        }
        return null;
    }

    @Override
    public StormTopology getTopology(String id) {
        try {
            return state.getNimbus().getTopology(id);
        } catch (TException e) {
            LOG.error("fail to get topology of topologId: " + id, e);
        }
        return null;
    }

    @Override
    public ClusterSummary getClusterInfo() {
        try {
            return state.getNimbus().getClusterInfo();
        } catch (TException e) {
            LOG.error("fail to get cluster info", e);
        }
        return null;
    }

    @Override
    public TopologyInfo getTopologyInfo(String id) {
        try {
            return state.getNimbus().getTopologyInfo(id);
        } catch (TException e) {
            LOG.error("fail to get topology info of topologyId: " + id, e);
        }
        return null;
    }

    /***
     * You should use getLocalClusterMap() to instead.This function will always return null
     * */
    @Deprecated
    @Override
    public Map getState() {
        return null;
    }

    public LocalClusterMap getLocalClusterMap() {
        return state;
    }

    public static void main(String[] args) throws Exception {
        LocalCluster localCluster = null;
        try {
            localCluster = new LocalCluster();
        } finally {
            if (localCluster != null) {
                localCluster.shutdown();
            }
        }
    }

    @Override
    public void uploadNewCredentials(String topologyName, Credentials creds) {
        try {
            state.getNimbus().uploadNewCredentials(topologyName, creds);
        } catch (Exception e) {
            LOG.error("fail to uploadNewCredentials of topologyId: " + topologyName, e);
        }
    }

}
