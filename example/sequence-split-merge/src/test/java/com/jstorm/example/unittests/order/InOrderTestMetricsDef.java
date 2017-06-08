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
package com.jstorm.example.unittests.order;

/**
 * @author binyang.dby on 2016/7/9.
 *
 * I use endsWith to filter the user define metrics from all metrics. Make sure all the metrics ends with ".user.define"
 */
public class InOrderTestMetricsDef
{
    public final static String METRIC_SPOUT_EMIT = "metric.order.spout.emit.user.define";
    public final static String METRIC_BOLT_SUCCESS = "metric.order.bolt.success.user.define";
    public final static String METRIC_BOLT_FAIL = "metric.order.bolt.fail.user.define";
}
