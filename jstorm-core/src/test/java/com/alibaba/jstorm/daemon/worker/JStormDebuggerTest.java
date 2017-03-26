/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.daemon.worker;

import backtype.storm.Config;
import com.alibaba.jstorm.client.ConfigExtension;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class JStormDebuggerTest {

	private final double SAMPLE_RATE = 0.1d;
    Random random;
    Map conf;

    @Before
    public void setUp() throws Exception {
        conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        ConfigExtension.setTopologyDebugRecvTuple(conf, true);
        ConfigExtension.setTopologyDebugSampleRate(conf, SAMPLE_RATE);
        JStormDebugger.update(conf);

        random = new Random(System.currentTimeMillis());
    }

    @Test
    public void testIsDebugDefault() throws Exception {
        ConfigExtension.setTopologyDebugSampleRate(conf, 1.0);
        JStormDebugger.update(conf);

        int tries = 50000;
        int lottery = 0;
        int num = tries;
        while (num-- > 0) {
            Long root_id = random.nextLong();
            if (JStormDebugger.isDebug(root_id)) {
                lottery++;
            }
        }
        assertEquals(tries, lottery, 0);


    }

    @Test
    public void testIsDebugForLong() throws Exception {
        int tries = 50000;
        double expected = tries * SAMPLE_RATE;
        int lottery = 0;
        int num = tries;
        while (num-- > 0) {
            Long root_id = random.nextLong();
            if (JStormDebugger.isDebug(root_id)) {
                lottery++;
            }
        }
        assertEquals(expected, lottery, expected * 0.05);
    }

    @Test
    public void testIsDebugForObject() throws Exception {
        boolean actual = JStormDebugger.isDebug("abc");
        assertEquals(false, actual);
        actual = JStormDebugger.isDebug((Object) null);
        assertEquals(false, actual);

        int tries = 50000;
        double expected = tries * SAMPLE_RATE;
        int lottery = 0;
        int num = tries;
        while (num-- > 0) {
            Long root_id = random.nextLong();
            if (JStormDebugger.isDebug((Object) root_id)) {
                lottery++;
            }
        }
        assertEquals(expected, lottery, expected * 0.1);

    }


}
