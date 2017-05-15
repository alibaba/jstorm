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
package backtype.storm.utils;

import java.util.Map;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormConfig;

import backtype.storm.LocalCluster;
import backtype.storm.generated.Nimbus.Iface;

public class NimbusClientWrapper implements StormObject {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClientWrapper.class);
    private final AtomicBoolean isValid = new AtomicBoolean(true);

    Iface client;
    NimbusClient remoteClient;
    Map conf;
    boolean isLocal = false;

    @Override
    public void init(Map conf) throws Exception {
        this.conf = conf;
        isLocal = StormConfig.try_local_mode(conf);

        if (isLocal) {
            client = LocalCluster.getInstance().getLocalClusterMap().getNimbus();
        } else {
            remoteClient = NimbusClient.getConfiguredClient(conf);
            client = remoteClient.getClient();
        }
        isValid.set(true);
    }

    public void invalidate() {
        isValid.set(false);
    }

    public boolean isValid() {
        return this.isValid.get();
    }

    @Override
    public void cleanup() {
        invalidate();
        if (remoteClient != null) {
            remoteClient.close();
        }
    }

    public Iface getClient() {
        return client;
    }

    public void reconnect() {
        cleanup();
        try {
            init(this.conf);
        } catch (Exception ex) {
            LOG.error("reconnect error, maybe nimbus is not alive.", ex);
        }
    }

}
