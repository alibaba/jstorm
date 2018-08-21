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

import java.util.Map;

/**
 * @author binyang.dby on 2016/7/8.
 *
 * This interface is used for check if a unit test should pass after running. The abstract classes which
 * implements this interface (like JStormUnitTestMetricValidator etc.) is recommended.
 */
public interface JStormUnitTestValidator
{
    /**
     * Validate is pass a unit test or not. Use assert in the body of this method since the return
     * value of it doesn't determine the test result of the unit test, it is just passed to the
     * return value of JStormUnitTestRunner.submitTopology()
     *
     * @param config the config which is passed in JStormUnitTestRunner.submitTopology()
     * @return the return value will be pass to the return value of JStormUnitTestRunner.submitTopology()
     */
    public boolean validate(Map config);
}
