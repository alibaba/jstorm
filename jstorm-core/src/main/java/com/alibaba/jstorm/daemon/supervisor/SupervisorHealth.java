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
package com.alibaba.jstorm.daemon.supervisor;

import backtype.storm.command.HealthCheck;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author xiaojian.fxj
 * @since 2.1.0
 */
public class SupervisorHealth extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(SupervisorHealth.class);

    @SuppressWarnings("unused")
    private Map<Object, Object> conf;
    private final int frequency;
    private AtomicBoolean active = null;

    private Heartbeat heartbeat;

    @SuppressWarnings("unchecked, unused")
    public SupervisorHealth(Map conf, Heartbeat heartbeat, String supervisorId) {
        this.conf = conf;
        this.frequency = ConfigExtension.getSupervisorFrequencyCheck(conf);
        long timeOut = ConfigExtension.getStormHealthTimeoutMs(conf);
        this.active = new AtomicBoolean(true);
        this.heartbeat = heartbeat;
        String errorDir = ConfigExtension.getStormMachineResourceErrorCheckDir(conf);
        String panicDir = ConfigExtension.getStormMachineResourcePanicCheckDir(conf);
        String warnDir = ConfigExtension.getStormMachineResourceWarningCheckDir(conf);

        LOG.info("start supervisor health check , timeout is " + timeOut + ", scripts directories are: " + panicDir
                 + ";" + errorDir + ";" + warnDir);
    }

    @SuppressWarnings("unchecked")
    public void updateStatus() {
        heartbeat.updateHealthStatus(HealthCheck.check());
    }

    @Override
    public Object getResult() {
        Integer result;
        if (active.get()) {
            result = frequency;
        } else {
            result = -1;
        }
        return result;
    }

    @Override
    public void run() {
        updateStatus();
    }

    public void setActive(boolean active) {
        this.active.getAndSet(active);
    }
}
