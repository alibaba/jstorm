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
package com.alibaba.jstorm.daemon.worker.hearbeat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

public class SyncContainerHb extends RunnableCallback {
    private final static Logger LOG = LoggerFactory.getLogger(SyncContainerHb.class);

    private String readDir;
    private String writeDir;
    private int timeoutSeconds = 60;
    private int frequency = 10;
    private int reserverNum = 10;
    private int noContainerHbTimes = 0;
    private boolean isFirstRead = true;
    private static final int SECOND_MILLISECOND = 1000;
    private static final int MAX_NO_CONTAINER_HB_TIMES = 30;

    public void removeOld(List<String> fileList, String dir) {
        if (fileList.size() <= reserverNum) {
            // don't need to remove old files
            return;
        }

        int removeNum = fileList.size() - reserverNum;
        for (int i = 0; i < removeNum; i++) {
            String fileName = fileList.get(i);

            String filePath = dir + File.separator + fileName;
            try {
                PathUtils.rmpath(filePath);
            } catch (Exception e) {
                LOG.error("Failed to delete " + filePath, e);
            }
            LOG.info("Remove heartbeat file " + filePath);
        }
    }

    public void checkNoContainerHbTimes() {
        noContainerHbTimes++;
        if (noContainerHbTimes >= MAX_NO_CONTAINER_HB_TIMES) {
            LOG.info("It's long time no container heartbeat");
            throw new RuntimeException("It's long time no container heartbeat");
        }
    }

    public void handlReadDir() {
        if (StringUtils.isBlank(readDir)) {
            return;
        }

        File file = new File(readDir);
        if (!file.exists()) {
            LOG.info(readDir + " doesn't exist right now");
            checkNoContainerHbTimes();
            return;
        } else if (!file.isDirectory()) {
            String msg = readDir + " isn't dir";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        String[] files = file.list();
        if (files == null || files.length == 0) {
            LOG.info(readDir + " doesn't contain heartbeat files right now");
            checkNoContainerHbTimes();
            return;
        }

        noContainerHbTimes = 0;
        List<String> fileList = JStormUtils.mk_list(files);
        Collections.sort(fileList);

        // removeOld(fileList);

        String biggest = fileList.get(fileList.size() - 1);
        long now = System.currentTimeMillis() / SECOND_MILLISECOND;
        long hb = 0;
        try {
            hb = Long.valueOf(biggest);
        } catch (Exception e) {
            LOG.info("Heartbeat file " + biggest + " isn't a valid file, remove it");

            String path = readDir + File.separator + biggest;
            try {
                PathUtils.rmpath(path);
            } catch (Exception e1) {
                LOG.error("Failed to delete " + path, e1);
            }
        }

        if (now - hb > timeoutSeconds) {
            if (isFirstRead) {
                checkNoContainerHbTimes();
                return;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("It's long time no container's heartbeat, ");
            sb.append("ContainerDir:").append(readDir);
            sb.append(",last heartbeat:").append(biggest);
            LOG.error(sb.toString());

            throw new RuntimeException(sb.toString());
        } else {
            isFirstRead = false;
            LOG.info("Receive container heartbeat " + biggest);
        }
    }

    public void handleWriteDir() {
        if (StringUtils.isBlank(writeDir)) {
            return;
        }

        String seconds = String.valueOf(System.currentTimeMillis() / SECOND_MILLISECOND);
        String path = writeDir + File.separator + seconds;
        try {
            PathUtils.touch(path);
            LOG.info("Successfully touch " + path);
        } catch (IOException e) {
            LOG.error("Failed to touch " + path, e);
            throw new RuntimeException("Failed to touch " + path);
        }

        File file = new File(writeDir);
        String[] files = file.list();
        if (files == null || files.length == 0) {
            LOG.info(readDir + " doesn't contain heartbeat files right now");
            return;
        }

        List<String> fileList = JStormUtils.mk_list(files);
        Collections.sort(fileList);

        removeOld(fileList, writeDir);
    }

    @Override
    public void run() {
        handleWriteDir();
        handlReadDir();
    }

    @Override
    public void shutdown() {
        frequency = -1;
        LOG.info("Shutdown sync container thread");
    }

    public Object getResult() {
        return frequency;
    }

    public String getReadDir() {
        return readDir;
    }

    public void resetReadHeartbeats() {
        File file = new File(readDir);

        if (!file.exists()) {
            LOG.info("Read heartbeat directory hasn't been created " + readDir);
            return;
        } else if (!file.isDirectory()) {
            LOG.error(readDir + " isn't a directory ");
            throw new RuntimeException(readDir + " isn't a directory ");
        }

        String[] files = file.list();
        if (files == null) {
            LOG.info("Failed to list " + readDir);
            files = new String[]{};
        }
        for (String fileName : files) {
            String path = readDir + File.separator + fileName;

            try {
                PathUtils.rmr(path);
            } catch (IOException e) {
                LOG.error("Failed to remove " + path, e);
            }
        }
        LOG.info("Successfully reset read heartbeats " + readDir);
    }

    public void setReadDir(String readDir) {
        this.readDir = readDir;
        if (StringUtils.isBlank(readDir)) {
            LOG.warn("ReadDir is empty");
        } else {
            LOG.info("ReadDir is " + readDir);
        }
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public String getWriteDir() {
        return writeDir;
    }

    public void setWriteDir(String writeDir) {
        this.writeDir = writeDir;
        if (StringUtils.isBlank(writeDir)) {
            LOG.warn("writeDir is empty");
            return;
        } else {
            LOG.info("writeDir is " + writeDir);
        }

        File file = new File(writeDir);
        if (!file.exists()) {
            file.mkdirs();
            LOG.info("Create directory " + writeDir);
        } else if (!file.isDirectory()) {
            LOG.error(writeDir + " isn't a directory ");
            throw new RuntimeException(writeDir + " isn't a directory ");
        }
    }

    public int getReserverNum() {
        return reserverNum;
    }

    public void setReserverNum(int reserverNum) {
        this.reserverNum = reserverNum;
    }

    public static AsyncLoopThread mkInstance(String containerHbDir, String hbDir, int timeout, int frequency) {
        SyncContainerHb syncContainerHbThread = new SyncContainerHb();

        syncContainerHbThread.setReadDir(containerHbDir);
        syncContainerHbThread.setWriteDir(hbDir);
        syncContainerHbThread.setTimeoutSeconds(timeout);
        syncContainerHbThread.setFrequency(frequency);

        StringBuilder sb = new StringBuilder();
        sb.append("Run process under Apsara/Yarn container");
        sb.append("ContainerDir:").append(containerHbDir);
        sb.append("HBDir:").append(hbDir);
        sb.append(", timeout:").append(timeout);
        sb.append(",frequency:").append(frequency);
        LOG.info(sb.toString());

        return new AsyncLoopThread(syncContainerHbThread, true, Thread.NORM_PRIORITY, true);
    }

    public static AsyncLoopThread mkNimbusInstance(Map conf) throws IOException {
        boolean isEnable = ConfigExtension.isEnableContainerNimbus();
        if (!isEnable) {
            LOG.info("Run nimbus without Apsara/Yarn container");
            return null;
        }

        String containerHbDir = ConfigExtension.getContainerNimbusHearbeat();
        String hbDir = StormConfig.masterHearbeatForContainer(conf);
        int timeout = ConfigExtension.getContainerHeartbeatTimeoutSeconds(conf);
        int frequency = ConfigExtension.getContainerHeartbeatFrequence(conf);

        return mkInstance(containerHbDir, hbDir, timeout, frequency);
    }

    public static AsyncLoopThread mkSupervisorInstance(Map conf) throws IOException {
        boolean isEnableContainer = ConfigExtension.isEnableContainerSupervisor();
        if (isEnableContainer) {
            String containerHbDir = ConfigExtension.getContainerSupervisorHearbeat();
            String hbDir = StormConfig.supervisorHearbeatForContainer(conf);
            int timeout = ConfigExtension.getContainerHeartbeatTimeoutSeconds(conf);
            int frequency = ConfigExtension.getContainerHeartbeatFrequence(conf);

            return mkInstance(containerHbDir, hbDir, timeout, frequency);
        }

        boolean isWorkerAutomaticStop = ConfigExtension.isWorkerStopWithoutSupervisor(conf);
        if (isWorkerAutomaticStop) {
            String containerHbDir = null;
            String hbDir = StormConfig.supervisorHearbeatForContainer(conf);
            int timeout = ConfigExtension.getContainerHeartbeatTimeoutSeconds(conf);
            int frequency = ConfigExtension.getContainerHeartbeatFrequence(conf);

            return mkInstance(containerHbDir, hbDir, timeout, frequency);
        }

        LOG.info("Run Supervisor without Apsara/Yarn container");
        return null;
    }

    public static AsyncLoopThread mkWorkerInstance(Map conf) throws IOException {
        boolean isEnableContainer = ConfigExtension.isEnableContainerSupervisor();
        boolean isWorkerAutomaticStop = ConfigExtension.isWorkerStopWithoutSupervisor(conf);
        if (!isEnableContainer && !isWorkerAutomaticStop) {
            LOG.info("Run worker without Apsara/Yarn container");
            return null;
        }

        String containerHbDir = StormConfig.supervisorHearbeatForContainer(conf);
        String hbDir = null;
        int timeout = ConfigExtension.getContainerHeartbeatTimeoutSeconds(conf);
        int frequency = ConfigExtension.getContainerHeartbeatFrequence(conf);

        return mkInstance(containerHbDir, hbDir, timeout, frequency);
    }

}
