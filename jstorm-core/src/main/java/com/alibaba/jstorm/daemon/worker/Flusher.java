package com.alibaba.jstorm.daemon.worker;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public abstract class Flusher implements Runnable {
    protected long _flushIntervalMs;
    private static FlusherPool FLUSHER;

    public static void setFlusherPool(FlusherPool flusherPool) {
        FLUSHER = flusherPool;
    }

    public abstract void run();

    public void start() {
        FLUSHER.start(this, _flushIntervalMs);
    }

    public void close() {
        FLUSHER.stop(this, _flushIntervalMs);
    }
}