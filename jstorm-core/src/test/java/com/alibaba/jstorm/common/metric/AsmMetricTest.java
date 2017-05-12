package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author wuchong
 */
public class AsmMetricTest {

    @Test
    public void testFlush() throws Exception {
        AsmCounter counter = new AsmCounter();
        counter.setMetricName("mock@metric@name");
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int c = random.nextInt(10);
            counter.update(c);
        }

        counter.flush();

        assertEquals(4, counter.getSnapshots().size());
    }

    @Test
    public void testSample() {
        AsmMeter meter = new AsmMeter();
        int t = 0, f = 0;
        for (int i = 0; i < 100; i++) {
            if (meter.sample()) {
                t++;
            } else {
                f++;
            }
        }
        System.out.println(t + "," + f);
    }

    @Test
    public void testGetValue() {
        AsmCounter counter = new AsmCounter();
        for (int i = 0; i < 100; i++) {
            counter.inc();
        }
        long v = (Long) counter.getValue(AsmWindow.M1_WINDOW);
        Assert.assertEquals(100, v);
    }

    @Test
    public void testSearchMetrics() {
        String topologyId = "testTopology";
        String comp = "comp";
        String metricName = "testMetric";

        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, comp, 1, metricName, MetricType
                .COUNTER), new AsmCounter());
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, comp, 2, metricName, MetricType
                .COUNTER), new AsmCounter());

        List<AsmMetric> taskMetrics = JStormMetrics.search(metricName, MetaType.TASK, MetricType.COUNTER);
        Assert.assertEquals(2, taskMetrics.size());

        List<AsmMetric> compMetrics = JStormMetrics.search(metricName, MetaType.COMPONENT, MetricType.COUNTER);
        Assert.assertEquals(1, compMetrics.size());
    }
}