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
package com.alibaba.jstorm.config;

import backtype.storm.utils.NimbusClientWrapper;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.1
 */
public class SupervisorRefreshConfig extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorRefreshConfig.class);

    /**
     * storm conf, excluding yarn config
     */
    private Map stormConf = new HashMap();

    private Map supervisorConf = new HashMap<>();

    /**
     * storm yaml string, excluding yarn config
     */
    private String stormYaml;

    private NimbusClientWrapper nimbusClientWrapper;
    private final Random random = new Random(System.currentTimeMillis());
    private boolean enableSync;
    private final Integer refreshInterval;
    private final YarnConfigBlacklist yarnConfigBlacklist;

    /**
     * yarn config
     */
    private final String retainedYarnConfig;

    public SupervisorRefreshConfig(Map conf) {
        LOG.info("init SupervisorRefreshConfig thread...");
        this.yarnConfigBlacklist = YarnConfigBlacklist.getInstance(conf);

        try {
            this.enableSync = ConfigExtension.getClusterConfSyncEnabled(conf);

            String rawYaml = FileUtils.readFileToString(new File(LoadConf.getStormYamlPath()));
            this.stormYaml = JStormUtils.trimEnd(yarnConfigBlacklist.filterConfigIfNecessary(rawYaml));
            this.stormConf.putAll(conf);
            this.stormConf.putAll(LoadConf.loadYamlFromString(this.stormYaml));

            this.supervisorConf.putAll(LoadConf.loadYamlFromString(this.stormYaml));
            this.retainedYarnConfig = yarnConfigBlacklist.getRetainedConfig(rawYaml);
            LOG.info("retained yarn config:\n============================\n{}", retainedYarnConfig);
        } catch (IOException ex) {
            LOG.error("failed to read local storm.yaml!", ex);
            throw new RuntimeException(ex);
        }

        // check nimbus config every 20 ~ 30 sec
        this.refreshInterval = random.nextInt(10) + 20;
        LOG.info("done.");
    }

    @Override
    public void run() {
        try {
            if (!enableSync) {
                return;
            }

            if (this.nimbusClientWrapper == null) {
                this.nimbusClientWrapper = new NimbusClientWrapper();
                try {
                    this.nimbusClientWrapper.init(this.stormConf);
                } catch (Exception ex) {
                    LOG.error("init nimbus client wrapper error, maybe nimbus is not alive.");
                }
            }

            String nimbusYaml = JStormUtils.trimEnd(yarnConfigBlacklist.filterConfigIfNecessary(
                    this.nimbusClientWrapper.getClient().getStormRawConf()));
            Map nimbusConf = LoadConf.loadYamlFromString(nimbusYaml);

            if (nimbusYaml != null && !this.supervisorConf.equals(nimbusConf)) {
                Map newConf = LoadConf.loadYamlFromString(nimbusYaml);
                if (newConf == null) {
                    LOG.error("received invalid storm.yaml, skip...");
                } else {
                    MapDifference<?, ?> diff = Maps.difference(this.supervisorConf, nimbusConf);
                    LOG.debug("conf diff, left only:{}, right only:{}",
                            diff.entriesOnlyOnLeft(), diff.entriesOnlyOnRight());
                    LOG.debug("received nimbus config update, new config:\n{}", nimbusYaml);
                    this.stormYaml = nimbusYaml;

                    // append yarn config
                    nimbusYaml = JStormUtils.trimEnd(nimbusYaml + "\n" + retainedYarnConfig);

                    // backup config & overwrite current storm.yaml
                    RefreshableComponents.refresh(newConf);
                    LoadConf.backupAndOverwriteStormYaml(nimbusYaml);
                }
            }
        } catch (Exception ex) {
            LOG.error("failed to get nimbus conf, maybe nimbus is not alive.");
            this.nimbusClientWrapper.reconnect();
        }
    }

    @Override
    public Object getResult() {
        return refreshInterval;
    }
}
