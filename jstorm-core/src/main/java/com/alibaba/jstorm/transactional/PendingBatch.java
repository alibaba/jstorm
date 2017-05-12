package com.alibaba.jstorm.transactional;

import java.util.ArrayList;
import java.util.List;

public class PendingBatch {
    public List<byte[]> tuples = new ArrayList<>();
    protected final Object lock = new Object();
    protected volatile boolean isActive = true;

    public PendingBatch(long batchId) {
    }

    public void addTuples(byte[] data) {
        synchronized (lock) {
            tuples.add(data);
        }
    }

    public List<byte[]> getTuples() {
        List<byte[]> ret;
        synchronized (lock) {
            isActive = false;
            ret = tuples;
            tuples = null;
        }
        return ret;
    }

    public void removeTuples() {
        synchronized (lock) {
            tuples = null;
            isActive = false;
        }
    }

    public boolean isEmpty() {
        return !isActive && (tuples == null || tuples.size() == 0);
    }

    @Override
    public String toString() {
        return "isActive: " + isActive + ", Pending tuple size:" + (tuples != null ? tuples.size() : 0);
    }
}