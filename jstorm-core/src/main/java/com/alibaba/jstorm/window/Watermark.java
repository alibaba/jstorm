package com.alibaba.jstorm.window;

import java.io.Serializable;

/**
 * @author wange
 * @since 16/12/16
 */
public class Watermark implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long timestamp;

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
