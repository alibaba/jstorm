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
package com.alibaba.jstorm.daemon.worker;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessSimulator {
    private static Logger LOG = LoggerFactory.getLogger(ProcessSimulator.class);

    protected static final Object lock = new Object();

    /**
     * skip old function name: pid-counter
     */
    protected static ConcurrentHashMap<String, WorkerShutdown> processMap = new ConcurrentHashMap<>();

    /**
     * Register process handler old function name: register-process
     *
     * @param pid          process id
     * @param shutdownable worker shutdown handle
     */
    public static void registerProcess(String pid, WorkerShutdown shutdownable) {
        processMap.put(pid, shutdownable);
    }

    /**
     * Get process handle
     *
     * @param pid process id
     */
    protected static WorkerShutdown getProcessHandle(String pid) {
        return processMap.get(pid);
    }

    /**
     * Get all process handles
     */
    protected static Collection<WorkerShutdown> GetAllProcessHandles() {
        return processMap.values();
    }

    /**
     * Kill a process
     *
     * @param pid process id
     */
    public static void killProcess(String pid) {
        synchronized (lock) {
            LOG.info("Begin to kill process " + pid);

            WorkerShutdown shutdownHandle = getProcessHandle(pid);

            if (shutdownHandle != null) {
                shutdownHandle.shutdown();
            }

            processMap.remove(pid);

            LOG.info("Successfully killed process " + pid);
        }
    }

    /**
     * kill all handle old function name: kill-all-processes
     */
    public static void killAllProcesses() {
        Set<String> pids = processMap.keySet();
        for (String pid : pids) {
            killProcess(pid);
        }

        LOG.info("Successfully killed all processes");
    }
}
