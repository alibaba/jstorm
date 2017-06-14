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

public class TestTrident {

    /**
     * replaced by unit test.
     */
    //@Test
    public void testTridentMap() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentMapExample.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * still have some problem yet, if i add a limit to the spout, it crashes
     * when reach the limit.
     */
    //@Test
    public void testTridentMinMaxOfDevices() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentMinMaxOfDevicesTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test.
     */
    //@Test
    public void testTridentMinMaxOfVehicles() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentMinMaxOfVehiclesTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test.
     */
    //@Test
    public void testTridentReach() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentReach.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test.
     */
    //@Test
    public void testTridentWindowingInmemoryStore() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentWindowingInmemoryStoreTopology.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }

    /**
     * replaced by unit test.
     */
    //@Test
    public void testTridentWordCount() {
        System.out.println("\n\n\n!!!!!!!!!!!!!!Begin !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
        TridentWordCount.test();
        System.out.println("\n\n\n!!!!!!!!!!!!!!End !!!!!!!!!!!!!!\n\n\n"
                + Thread.currentThread().getStackTrace()[1].getMethodName());
                
    }
}
