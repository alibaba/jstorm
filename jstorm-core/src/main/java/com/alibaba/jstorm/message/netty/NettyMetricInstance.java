package com.alibaba.jstorm.message.netty;

import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMeter;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class NettyMetricInstance {

    private static Logger LOG = LoggerFactory.getLogger(NettyMetricInstance.class);
    private static final AtomicBoolean alreadyRegister = new AtomicBoolean(false);
    public static AsmMeter nettyServerRecvSpeed;
    public static AsmHistogram networkWorkerTransmitTime;
    public static AsmMeter totalSendSpeed;
    public static AsmHistogram batchSizeWorkerHistogram;

    public static synchronized void register() {
        if (alreadyRegister.compareAndSet(false, true)) {
            nettyServerRecvSpeed = (AsmMeter) JStormMetrics
                    .registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.NETTY_SRV_RECV_SPEED, MetricType.METER), new AsmMeter());
            networkWorkerTransmitTime = (AsmHistogram) JStormMetrics
                    .registerWorkerMetric(MetricUtils.workerMetricName(MetricDef.NETTY_SRV_MSG_TRANS_TIME, MetricType.HISTOGRAM), new AsmHistogram());
            totalSendSpeed = (AsmMeter) JStormMetrics
                    .registerWorkerTopologyMetric(JStormMetrics.workerMetricName(MetricDef.NETTY_CLI_SEND_SPEED, MetricType.METER), new AsmMeter());
            batchSizeWorkerHistogram = (AsmHistogram) JStormMetrics
                    .registerWorkerMetric(MetricUtils.workerMetricName(MetricDef.NETTY_CLI_BATCH_SIZE, MetricType.HISTOGRAM), new AsmHistogram());
            LOG.info("Successfully register the netty metrics {}, {}, {}, {}", nettyServerRecvSpeed.getMetricName(), networkWorkerTransmitTime.getMetricName(),
                    totalSendSpeed.getMetricName(), batchSizeWorkerHistogram.getMetricName());
        }
    }
}
