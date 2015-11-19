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
package com.alibaba.jstorm.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;

/**
 * storm utils
 * 
 * 
 * @author yannian/Longda/Xin.Zhou/Xin.Li
 * 
 */
public class JStormServerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JStormServerUtils.class);

    public static void downloadCodeFromMaster(Map conf, String localRoot, String masterCodeDir, String topologyId, boolean isSupervisor) throws IOException,
            TException {
        FileUtils.forceMkdir(new File(localRoot));
        FileUtils.forceMkdir(new File(StormConfig.stormlib_path(localRoot)));

        String localStormjarPath = StormConfig.stormjar_path(localRoot);
        String masterStormjarPath = StormConfig.stormjar_path(masterCodeDir);
        Utils.downloadFromMaster(conf, masterStormjarPath, localStormjarPath);

        String localStormcodePath = StormConfig.stormcode_path(localRoot);
        String masterStormcodePath = StormConfig.stormcode_path(masterCodeDir);
        Utils.downloadFromMaster(conf, masterStormcodePath, localStormcodePath);

        String localStormConfPath = StormConfig.stormconf_path(localRoot);
        String masterStormConfPath = StormConfig.stormconf_path(masterCodeDir);
        Utils.downloadFromMaster(conf, masterStormConfPath, localStormConfPath);

        Map stormConf = (Map) StormConfig.readLocalObject(topologyId, localStormConfPath);

        if (stormConf == null)
            throw new IOException("Get topology conf error: " + topologyId);

        List<String> libs = (List<String>) stormConf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
        if (libs == null)
            return;
        for (String libName : libs) {
            String localStromLibPath = StormConfig.stormlib_path(localRoot, libName);
            String masterStormLibPath = StormConfig.stormlib_path(masterCodeDir, libName);
            Utils.downloadFromMaster(conf, masterStormLibPath, localStromLibPath);
        }
    }

    public static void createPid(String dir) throws Exception {
        File file = new File(dir);

        if (file.exists() == false) {
            file.mkdirs();
        } else if (file.isDirectory() == false) {
            throw new RuntimeException("pid dir:" + dir + " isn't directory");
        }

        String[] existPids = file.list();

        // touch pid before
        String pid = JStormUtils.process_pid();
        String pidPath = dir + File.separator + pid;
        PathUtils.touch(pidPath);
        LOG.info("Successfully touch pid  " + pidPath);

        for (String existPid : existPids) {
            try {
                JStormUtils.kill(Integer.valueOf(existPid));
                PathUtils.rmpath(dir + File.separator + existPid);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }

    }

    public static void startTaobaoJvmMonitor() {
        // JmonitorBootstrap bootstrap = JmonitorBootstrap.getInstance();
        // bootstrap.start();
    }

    public static boolean isOnePending(Map conf) {
        Object pending = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if (pending == null) {
            return false;
        }

        int pendingNum = JStormUtils.parseInt(pending);
        if (pendingNum == 1) {
            return true;
        } else {
            return false;
        }
    }

    public static String getName(String componentId, int taskId) {
        return componentId + ":" + taskId;
    }

    public static String getHostName(Map conf) {
        String hostName = ConfigExtension.getSupervisorHost(conf);
        if (hostName == null) {
            hostName = NetWorkUtils.hostname();
        }

        if (ConfigExtension.isSupervisorUseIp(conf)) {
            hostName = NetWorkUtils.ip();
        }

        return hostName;
    }

};
