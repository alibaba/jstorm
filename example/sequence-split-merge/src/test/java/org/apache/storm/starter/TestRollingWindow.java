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
package org.apache.storm.starter;

import org.junit.Test;

import com.alipay.dw.jstorm.example.window.RollingTopWords;
import com.alipay.dw.jstorm.example.window.SkewedRollingTopWords;
import com.alipay.dw.jstorm.example.window.SlidingTupleTsTopology;
import com.alipay.dw.jstorm.example.window.SlidingWindowTopology;

public class TestRollingWindow {
    /**
     * replaced by unit test
     */
    //@Test
    public void testRollingTopWords() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        RollingTopWords.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced
     */
    //@Test
    public void testSkewedRollingTopWords() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SkewedRollingTopWords.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced
     */
    //@Test
    public void testSlidingTupleTs() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SlidingTupleTsTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    /**
     * replaced by unit test
     */
    //@Test
    public void testSlidingWindowTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        SlidingWindowTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
    }
    
}
