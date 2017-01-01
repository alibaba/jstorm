package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.config.Refreshable;
import com.alibaba.jstorm.config.RefreshableComponents;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wange
 * @since 16/7/7
 */
public abstract class BaseMetricUploaderWithFlowControl implements MetricUploader, Refreshable {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile int maxConcurrentUploadingNum;
    private final AtomicInteger currentUploadingNum = new AtomicInteger(0);

    public BaseMetricUploaderWithFlowControl() {
        RefreshableComponents.registerRefreshable(this);
    }

    public void setMaxConcurrentUploadingNum(int maxConcurrentUploadingNum) {
        this.maxConcurrentUploadingNum = maxConcurrentUploadingNum;
    }

    public void incrUploadingNum() {
        int num = currentUploadingNum.incrementAndGet();
        logger.debug("incr, UploadingNum:{}", num);
    }

    public void decrUploadingNum() {
        int num = currentUploadingNum.decrementAndGet();
        logger.debug("decr, UploadingNum:{}", num);
    }

    public synchronized boolean syncToUpload() {
        if (currentUploadingNum.get() < maxConcurrentUploadingNum) {
            incrUploadingNum();
            return true;
        }
        return false;
    }

    @Override
    public void refresh(Map conf) {
        int maxUploadingNum = ConfigExtension.getMaxConcurrentUploadingNum(conf);
        if (maxUploadingNum > 0 && maxUploadingNum != this.maxConcurrentUploadingNum) {
            this.maxConcurrentUploadingNum = maxUploadingNum;
        }
    }
}
