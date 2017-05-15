/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.common.metric.old.window;

import junit.framework.TestCase;

/**
 * @author wuchong on 15/8/11.
 */
public class StatBucketsTest extends TestCase {

    public void testPrettyUptime() throws Exception {
        int secs = 10860;
        assertEquals("0d3h1m0s", StatBuckets.prettyUptimeStr(secs));

        secs = 203010;
        assertEquals("2d8h23m30s", StatBuckets.prettyUptimeStr(secs));

        secs = 234;
        assertEquals("0d0h3m54s", StatBuckets.prettyUptimeStr(secs));

        secs = 32;
        assertEquals("0d0h0m32s", StatBuckets.prettyUptimeStr(secs));

        secs = 0;
        assertEquals("0d0h0m0s", StatBuckets.prettyUptimeStr(secs));

    }
}