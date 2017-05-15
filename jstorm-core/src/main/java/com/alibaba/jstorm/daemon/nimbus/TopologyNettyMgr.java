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
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.nimbus.NimbusInfo;
import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.metric.MetricUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TopologyNettyMgr {
    private static Logger LOG = LoggerFactory.getLogger(TopologyNettyMgr.class);
    private Map nimbusConf;
    private ConcurrentHashMap<String, Boolean> setting = new ConcurrentHashMap<>();

    public TopologyNettyMgr(Map conf) {
        nimbusConf = conf;
    }

    protected boolean getTopology(Map conf) {
        return MetricUtils.isEnableNettyMetrics(conf);
    }

    public boolean getTopology(String topologyId) {
        BlobStore blobStore = null;
        try {
            String topologyName = Common.topologyIdToName(topologyId);
            Boolean isEnable = setting.get(topologyName);
            if (isEnable != null) {
                return isEnable;
            }

            blobStore = BlobStoreUtils.getNimbusBlobStore(nimbusConf, NimbusInfo.fromConf(nimbusConf));
            Map topologyConf = StormConfig.read_nimbus_topology_conf(topologyId, blobStore);

            isEnable = getTopology(topologyConf);
            setting.put(topologyName, isEnable);
            LOG.info("{} netty metrics setting is {}", topologyName, isEnable);
            return isEnable;

        } catch (Exception e) {
            LOG.info("Failed to get {} netty metrics setting ", topologyId);
            return true;
        } finally {
            if (blobStore != null) {
                blobStore.shutdown();
                blobStore = null;
            }
        }
    }

    public void setTopology(Map conf) {
        String topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (topologyName == null) {
            LOG.info("No topologyName setting");
            return;
        }
        boolean isEnable = getTopology(conf);
        setting.put(topologyName, isEnable);

        LOG.info("{} netty metrics setting is {}", topologyName, isEnable);
    }

    public void rmTopology(String topologyId) {
        String topologyName;
        try {
            topologyName = Common.topologyIdToName(topologyId);
            setting.remove(topologyName);
            LOG.info("Remove {} netty metrics setting ", topologyName);
        } catch (InvalidTopologyException ignored) {
        }
    }
}
