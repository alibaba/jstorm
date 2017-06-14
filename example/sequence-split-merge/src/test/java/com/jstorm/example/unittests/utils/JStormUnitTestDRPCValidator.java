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
package com.jstorm.example.unittests.utils;

import backtype.storm.LocalDRPC;

/**
 * @author binyang.dby on 2016/7/21.
 *
 * used for validate if a unit test result should pass by execute local DRPC.
 */
public abstract class JStormUnitTestDRPCValidator implements JStormUnitTestValidator
{
    private LocalDRPC localDRPC;

    /**
     * pass a localDRPC object to execute when you call executeLocalDRPC() method.
     * @param localDRPC the localDRPC
     */
    public JStormUnitTestDRPCValidator(LocalDRPC localDRPC)
    {
        this.localDRPC = localDRPC;
    }

    /**
     * execute local DRPC and generate a result
     * @param function the name of function
     * @param args the args
     * @return the execute result
     */
    protected String executeLocalDRPC(String function, String args) {
        return localDRPC.execute(function, args);
    }
}
