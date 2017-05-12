package com.alibaba.jstorm.window;

import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Collection;

/**
 * A retractor enables re-computing of purged historic windows.
 * Note that for such re-computing, user would need to pull window states from their own state factory,
 * e.g., HBase, HDFS, DB, etc. instead of state factory within JStorm.
 *
 * @author wange
 * @since 16/12/19
 */
public interface Retractor extends Serializable {
    void retract(Tuple element, Collection<TimeWindow> windows);
}
