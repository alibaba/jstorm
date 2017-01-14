/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.daemon.worker;

import backtype.storm.Config;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.config.Refreshable;
import com.alibaba.jstorm.config.RefreshableComponents;
import com.alibaba.jstorm.utils.JStormUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * manage the global state of topology.debug & topology.debug.recv.tuple & topology.debug.sample.rate on the fly
 *
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class JStormDebugger implements Refreshable {
    private static Logger LOG = LoggerFactory.getLogger(JStormDebugger.class);

    private JStormDebugger() {
        RefreshableComponents.registerRefreshable(this);
    }

    private static final JStormDebugger INSTANCE = new JStormDebugger();

    public static JStormDebugger getInstance() {
        return INSTANCE;
    }

    public static volatile boolean isDebug = false;
    public static volatile boolean isDebugRecv = false;
    public static volatile double sampleRate = 1.0d;

    // PRECISION represent the sampling rate precision , such as: 10000 -> 0.0001
    private static final int PRECISION = 10000;


    public static boolean isDebug(Long root_id) {
        return isDebug && sample(root_id);
    }

    public static boolean isDebug(Set<Long> root_ids) {
        return isDebug && sample(root_ids);
    }

    public static boolean isDebug(Collection<Tuple> anchors) {
        return isDebug && sample(anchors);
    }

    public static boolean isDebug(Object id) {
        return isDebug && sample(id);
    }

    public static boolean isDebugRecv(MessageId msgId) {
        return msgId != null && isDebugRecv(msgId.getAnchors());
    }

    public static boolean isDebugRecv(Set<Long> root_ids) {
        return isDebugRecv && sample(root_ids);
    }

    public static boolean isDebugRecv(Collection<Tuple> anchors) {
        return isDebugRecv && sample(anchors);
    }

    public static boolean isDebugRecv(Object id) {
        return isDebugRecv && sample(id);
    }

    /**
     * the tuple debug logs only output `rate`% the tuples with same root_id, should be logged or not logged together.
     */
    private static boolean sample(Long root_id) {
        if (Double.compare(sampleRate, 1.0d) >= 0)
            return true;
        int mod = (int) (Math.abs(root_id) % PRECISION);
        int threshold = (int) (sampleRate * PRECISION);
        return mod < threshold;
    }

    /**
     * one of the root_ids has been chosen , the logs should be output
     */
    private static boolean sample(Set<Long> root_ids) {
        if (Double.compare(sampleRate, 1.0d) >= 0)
            return true;
        int threshold = (int) (sampleRate * PRECISION);
        for (Long id : root_ids) {
            int mod = (int) (Math.abs(id) % PRECISION);
            if (mod < threshold) {
                return true;
            }
        }
        return false;
    }

    /**
     * one of the tuples has been chosen, the logs should be output
     */
    private static boolean sample(Collection<Tuple> anchors) {
        if (Double.compare(sampleRate, 1.0d) >= 0)
            return true;
        for (Tuple t : anchors) {
            if (sample(t.getMessageId().getAnchors())) {
                return true;
            }
        }
        return false;
    }

    private static boolean sample(Object id) {
        if (Double.compare(sampleRate, 1.0d) >= 0)
            return true;
        return id != null && id instanceof Long && sample((Long) id);
    }

    public static void update(Map conf) {
        boolean _isDebug = JStormUtils.parseBoolean(conf.get(Config.TOPOLOGY_DEBUG), isDebug);
        if (_isDebug != isDebug) {
            isDebug = _isDebug;
            LOG.info("switch topology.debug to {}", _isDebug);
        }
        boolean _isDebugRecv = ConfigExtension.isTopologyDebugRecvTuple(conf);
        if (_isDebugRecv != isDebugRecv) {
            isDebugRecv = _isDebugRecv;
            LOG.info("switch topology.debug.recv.tuple to {}", _isDebug);
        }
        double _sampleRate = ConfigExtension.getTopologyDebugSampleRate(conf);
        if (Double.compare(_sampleRate, sampleRate) != 0) {
            sampleRate = _sampleRate;
            LOG.info("switch topology.debug.sample.rate to {}", _sampleRate);
        }
    }

    @Override
    public void refresh(Map conf) {
        update(conf);
    }
}
