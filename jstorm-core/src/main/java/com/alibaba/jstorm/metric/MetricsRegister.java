package com.alibaba.jstorm.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.NimbusClientWrapper;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.1.2
 */
public class MetricsRegister {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private Map conf;
    private String topologyId;
    private NimbusClientWrapper client = null;
    private final Object lock = new Object();

    public MetricsRegister(Map conf, String topologyId) {
        this.conf = conf;
        this.topologyId = topologyId;
    }

    public Map<String, Long> registerMetrics(Set<String> names) {
        if (!JStormMetrics.enabled) {
            return new HashMap<>();
        }
        try {
            synchronized (lock){
                if (client == null) {
                    client = new NimbusClientWrapper();
                    client.init(conf);
                }
            }
            return client.getClient().registerMetrics(topologyId, names);
        } catch (Exception e) {
            LOG.error("Failed to gen metric ids", e);
            if (client != null) {
                client.cleanup();
                client = null;
            }
        }

        return new HashMap<>();
    }

}
