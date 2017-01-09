package com.alibaba.jstorm.daemon.nimbus.metric;

import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.MetricQueryClient;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.TimeUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckMetricEvent extends MetricEvent {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private TopologyMetricContext tmContext;
    private List<Pair<MetricMeta, Long>> pairs;

    @Override
    public void run() {
        try {
            MetricQueryClient metricQueryClient = context.getMetricQueryClient();
            int batch = 30;
            long start = System.currentTimeMillis() - batch * AsmWindow.M1_WINDOW * TimeUtils.MS_PER_SEC;
            long end = System.currentTimeMillis();
            List<MetricMeta> toDelete = new ArrayList<>();
            for (Pair<MetricMeta, Long> pair : pairs) {
                MetricMeta meta = pair.getFirst();
                MetricType metricType = MetricType.parse(meta.getMetricType());

                Long id1 = meta.getId();
                Long id2 = pair.getSecond();
                List<Object> data1 = metricQueryClient.getMetricData(id1,
                        metricType, AsmWindow.M1_WINDOW, start, end, batch);
                List<Object> data2 = metricQueryClient.getMetricData(id2,
                        metricType, AsmWindow.M1_WINDOW, start, end, batch);

                int size1 = data1 != null ? data1.size() : 0;
                int size2 = data2 != null ? data2.size() : 0;

                LOG.info("check duplicate metric meta for {}, id1:{}, size:{}, id2:{}, size:{}",
                        meta.getMetricName(), id1, size1, id2, size2);
                if (size1 > size2) { // delete id2
                    meta.setId(id2);
                    toDelete.add(meta);
                    tmContext.getMemMeta().put(meta.getFQN(), id1);
                } else { // delete id1
                    toDelete.add(meta);
                    tmContext.getMemMeta().put(meta.getFQN(), id2);
                }
            }

            if (toDelete.size() > 0) {
                LOG.warn("will delete the following duplicate metric meta\n----------------------------\n");
                for (MetricMeta meta : toDelete) {
                    LOG.warn("metric:{}, id:{}", meta.getFQN(), meta.getId());
                }
                LOG.info("\n");
                metricQueryClient.deleteMeta(toDelete);
            }
            //context.getMetricCache().putMeta(topologyId, tmContext.getMemMeta());
            //context.getStormClusterState().set_topology_metric(topologyId, zkSize - dupMetaList.size());
        } catch (Exception ex) {
            LOG.error("Error", ex);
        }
    }

    public static void pushEvent(String topologyId, TopologyMetricContext tmContext,
                                 List<Pair<MetricMeta, Long>> metaPairs) {
        CheckMetricEvent event = new CheckMetricEvent();
        event.topologyId = topologyId;
        event.tmContext = tmContext;
        event.pairs = metaPairs;

        ClusterMetricsRunnable.pushEvent(event);
    }
}
