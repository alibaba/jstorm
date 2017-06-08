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

import org.apache.storm.starter.trident.TridentMapExample;
import org.apache.storm.starter.trident.TridentMinMaxOfDevicesTopology;
import org.apache.storm.starter.trident.TridentMinMaxOfVehiclesTopology;
import org.apache.storm.starter.trident.TridentReach;
import org.apache.storm.starter.trident.TridentWindowingInmemoryStoreTopology;
import org.apache.storm.starter.trident.TridentWordCount;
import org.junit.Test;

import com.alipay.dw.jstorm.example.batch.SimpleBatchTopology;
import com.alipay.dw.jstorm.example.performance.test.FastWordCountTopology;
import com.alipay.dw.jstorm.example.update.topology.TestBackpressure;
import com.alipay.dw.jstorm.example.window.RollingTopWords;
import com.alipay.dw.jstorm.example.window.SkewedRollingTopWords;
import com.alipay.dw.jstorm.example.window.SlidingTupleTsTopology;
import com.alipay.dw.jstorm.example.window.SlidingWindowTopology;
import com.alipay.dw.jstorm.transcation.TransactionalGlobalCount;
import com.alipay.dw.jstorm.transcation.TransactionalWords;

/**
 * keep this unit test case
 * maybe we should add some assert.
 */
public class TestDrpc {

    /**
     * replaced
     */
    //@Test
    public void testBasicDRPCTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        BasicDRPCTopology.testDrpc();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced
     */
    //@Test
    public void testManualDRPC() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        ManualDRPC.testDrpc();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test in package performance
     */
    //@Test
    public void testFastWordCountTopology() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        FastWordCountTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
    
}
