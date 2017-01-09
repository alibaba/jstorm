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
