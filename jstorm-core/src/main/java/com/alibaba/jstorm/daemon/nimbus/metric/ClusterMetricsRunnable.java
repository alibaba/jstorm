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
package com.alibaba.jstorm.daemon.nimbus.metric;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;

public class ClusterMetricsRunnable extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterMetricsRunnable.class);

    protected final BlockingDeque<MetricEvent> queue = new LinkedBlockingDeque<>();

    private ClusterMetricsContext context;
    private NimbusData nimbusData;
    private static ClusterMetricsRunnable instance;

    private ClusterMetricsRunnable(NimbusData nimbusData) {
        this.nimbusData = nimbusData;
        context = new ClusterMetricsContext(nimbusData);
    }

    public void init() {
        context.init();
    }

    public static ClusterMetricsRunnable mkInstance(NimbusData nimbusData) {
        synchronized (ClusterMetricsRunnable.class) {
            if (instance == null) {
                instance = new ClusterMetricsRunnable(nimbusData);
            }
        }

        return instance;
    }

    public static ClusterMetricsRunnable getInstance() {
        return instance;
    }

    public static void pushEvent(MetricEvent event) {
        instance.queue.offer(event);
    }

    @Override
    public void run() {
        MetricEvent event = null;
        try {
            event = queue.take();
        } catch (InterruptedException ignored) {
        }
        if (event != null) {
            event.setClusterMetricsContext(context);
            nimbusData.getScheduExec().submit(event);
        }
    }

    @Override
    public void shutdown() {
        if (context != null) {
            context.shutdown();
        }
        
        instance = null;
    }

    public ClusterMetricsContext getContext() {
        return context;
    }
}
