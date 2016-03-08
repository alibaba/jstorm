package com.alibaba.jstorm.common.metric.old.window;

import junit.framework.TestCase;

/**
 * Created by wuchong on 15/8/11.
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