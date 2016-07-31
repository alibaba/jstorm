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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.nimbus.DefaultInimbus;
import com.alibaba.jstorm.daemon.nimbus.NimbusServer;
import com.alibaba.jstorm.daemon.supervisor.Supervisor;
import com.alibaba.jstorm.message.netty.NettyContext;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.zk.Factory;
import com.alibaba.jstorm.zk.Zookeeper;

public class LocalUtils {

    public static Logger LOG = LoggerFactory.getLogger(LocalUtils.class);

    public static LocalClusterMap prepareLocalCluster() {
        LocalClusterMap state = new LocalClusterMap();
        try {
            List<String> tmpDirs = new ArrayList();

            String zkDir = getTmpDir();
            tmpDirs.add(zkDir);
            Factory zookeeper = startLocalZookeeper(zkDir);
            Map conf = getLocalConf(zookeeper.getZooKeeperServer().getClientPort());

            String nimbusDir = getTmpDir();
            tmpDirs.add(nimbusDir);
            Map nimbusConf = deepCopyMap(conf);
            nimbusConf.put(Config.STORM_LOCAL_DIR, nimbusDir);
            NimbusServer instance = new NimbusServer();

            Map supervisorConf = deepCopyMap(conf);
            String supervisorDir = getTmpDir();
            tmpDirs.add(supervisorDir);
            supervisorConf.put(Config.STORM_LOCAL_DIR, supervisorDir);
            Supervisor supervisor = new Supervisor();
            IContext context = getLocalContext(supervisorConf);

            state.setNimbusServer(instance);
            state.setNimbus(instance.launcherLocalServer(nimbusConf, new DefaultInimbus()));
            state.setZookeeper(zookeeper);
            state.setConf(conf);
            state.setTmpDir(tmpDirs);
            state.setSupervisor(supervisor.mkSupervisor(supervisorConf, context));
            return state;
        } catch (Exception e) {
            LOG.error("prepare cluster error!", e);
            state.clean();

        }
        return null;
    }

    public static Factory startLocalZookeeper(String tmpDir) {
        for (int i = 2000; i < 65535; i++) {
            try {
                return Zookeeper.mkInprocessZookeeper(tmpDir, i);
            } catch (Exception e) {
                LOG.error("fail to launch zookeeper at port: " + i, e);
            }
        }
        throw new RuntimeException("No port is available to launch an inprocess zookeeper.");
    }

    public static String getTmpDir() {
        return System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID();
    }
    
    public static Map getLocalBaseConf() {

        JStormUtils.setLocalMode(true);
        
        Map conf = new HashMap();
        
        conf.put(Config.STORM_CLUSTER_MODE, "local");
        
        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
        conf.put(Config.ZMQ_LINGER_MILLIS, 0);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
        conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
        ConfigExtension.setSpoutDelayRunSeconds(conf, 0);
        ConfigExtension.setTaskCleanupTimeoutSec(conf, 0);
        
        ConfigExtension.setTopologyDebugRecvTuple(conf, true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        
        conf.put(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE, false);
        return conf;
    }

    public static Map getLocalConf(int port) {
        Map conf = Utils.readStormConfig();
        conf.putAll(getLocalBaseConf());
        
        List<String> zkServers = new ArrayList<String>(1);
        zkServers.add("localhost");
        
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
        conf.put(Config.STORM_ZOOKEEPER_PORT, port);
        
        return conf;
    }

    private static IContext getLocalContext(Map conf) {
        if (!(Boolean) conf.get(Config.STORM_LOCAL_MODE_ZMQ)) {
            IContext result = new NettyContext();
            ConfigExtension.setLocalWorkerPort(conf, 6800);
            result.prepare(conf);
            return result;
        }
        return null;
    }

    private static Map deepCopyMap(Map map) {
        return new HashMap(map);
    }
}
