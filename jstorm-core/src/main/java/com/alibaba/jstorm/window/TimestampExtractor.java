package com.alibaba.jstorm.window;

import backtype.storm.tuple.Tuple;
import java.io.Serializable;

/**
 * @author wange
 * @since 16/12/16
 */
public interface TimestampExtractor extends Serializable {
    long extractTimestamp(Tuple element);
}
