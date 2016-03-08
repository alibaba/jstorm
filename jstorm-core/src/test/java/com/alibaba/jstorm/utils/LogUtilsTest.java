/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.utils;


import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class LogUtilsTest {


    @Test
    public void testSetLogBackLevel() throws Exception {
        assertEquals(true,  LogUtils.setLogBackLevel(null,   null));      // Set ROOT logger level to 'OFF'
        assertEquals(true,  LogUtils.setLogBackLevel(null,   "TRACE"));   // Set ROOT logger level to TRACE
        assertEquals(true,  LogUtils.setLogBackLevel(".",    "DEBUG"));
        assertEquals(true,  LogUtils.setLogBackLevel(null,   "INFO"));
        assertEquals(true,  LogUtils.setLogBackLevel(null,   "WARN"));
        assertEquals(false, LogUtils.setLogBackLevel(null,   "WARNING")); // No such log level
        assertEquals(false, LogUtils.setLogBackLevel(null,   ""));        // No such log level
        assertEquals(false, LogUtils.setLogBackLevel(null,   "-"));       // No such log level
        assertEquals(true,  LogUtils.setLogBackLevel("org.", "DEBUG"));
    }

//    @Test
    public void testSetLog4jLevel() throws Exception {
        assertEquals(true,  LogUtils.setLog4jLevel(null,   null));      // Set ROOT logger level to 'OFF'
        assertEquals(true,  LogUtils.setLog4jLevel(null,   "TRACE"));   // Set ROOT logger level to TRACE
        assertEquals(true,  LogUtils.setLog4jLevel(".",    "DEBUG"));
        assertEquals(true,  LogUtils.setLog4jLevel(null,   "INFO"));
        assertEquals(true,  LogUtils.setLog4jLevel(null,   "WARN"));
        assertEquals(false, LogUtils.setLog4jLevel(null,   "WARNING"));
        assertEquals(false, LogUtils.setLog4jLevel(null,   ""));
        assertEquals(false, LogUtils.setLog4jLevel(null,   "-"));
        assertEquals(true,  LogUtils.setLog4jLevel("com.alibaba.jstorm", "TRACE"));
        assertEquals(true,  LogUtils.setLog4jLevel("a.b.c", "DEBUG"));
    }

}