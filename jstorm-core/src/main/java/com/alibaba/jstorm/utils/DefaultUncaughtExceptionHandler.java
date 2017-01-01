package com.alibaba.jstorm.utils;

import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiaojian.fxj on 2015/12/15.
 */
public class DefaultUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultUncaughtExceptionHandler.class);
    @Override
    public void uncaughtException(Thread a, Throwable e) {
        try {
            Utils.handleUncaughtException(e);
        }catch (Error error){
            LOG.info("Received error in main thread.. terminating server...", error);
            Runtime.getRuntime().exit(-2);
        }
    }
}
