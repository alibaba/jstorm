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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/8.
 *
 * This is a util for JStorm unit tests. You can pass in a topology , a config, the runtime, and
 * an optional validator to run a unit test.
 * The topology is always run in local cluster mode since it is just used for unit tests.
 * The minimum runtime is 120 seconds because the metrics need time to update if you use it. The
 * map and the validator can be null.
 */
public class JStormUnitTestRunner
{
    /**
     * submit a topology to run in local cluster mode and check if the result should
     * pass the unit test by a callback.
     * @param topology the topology to submit
     * @param runtimeSec max run time in seconds, minimum is 120s
     * @param validator the callback to invoke after cluster closed
     * @return the result of validator if set, or true if it is null
     */
    public static boolean submitTopology(StormTopology topology, Map config, int runtimeSec, JStormUnitTestValidator validator)
    {
        JStormUtils.sleepMs(15 * 1000);

        if(runtimeSec < 120)
            runtimeSec = 120;

        LocalCluster localCluster = new LocalCluster();
        String topologyName;

        if(config == null)
            config = new Config();

        if(config.containsKey(Config.TOPOLOGY_NAME))
            topologyName = (String)config.get(Config.TOPOLOGY_NAME);
        else
            topologyName = "JStormUnitTestTopology";

        localCluster.submitTopology(topologyName, config, topology);
        JStormUtils.sleepMs(runtimeSec * 1000);

        if(validator != null)
            return validator.validate(config);

        return true;
    }
}
