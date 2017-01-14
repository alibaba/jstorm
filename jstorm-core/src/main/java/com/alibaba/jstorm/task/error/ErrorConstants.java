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

package com.alibaba.jstorm.task.error;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ErrorConstants {
    public static final String WARN = "warn";
    public static final String ERROR = "error";
    public static final String FATAL = "fatal";


    public static final int CODE_QUEUE_FULL = 100;
    /** executor queue is full, WARN level, duration 3min **/
    public static final int CODE_EXE_QUEUE = 101;
    /** serialize queue is full **/
    public static final int CODE_SER_QUEUE = 102;
    /** deserialize queue is full **/
    public static final int CODE_DES_QUEUE = 103;

    /** backpressure error, WARN level, default duration **/
    public static final int CODE_BP = 110;

    /** backpressure error, WARN level, default duration **/
    public static final int CODE_TASK_NO_RESPONSE = 120;

    /** task dead error, ERROR level, duration 3days **/
    public static final int CODE_TASK_DEAD = 300;

    /** worker is dead, FATAL level **/
    public static final int CODE_WORKER_DEAD = 500;
    /** worker time out, maybe out of memory, FATAL level **/
    public static final int CODE_WORKER_TIMEOUT = 501;
    /** worker exception, FATAL level **/
    public static final int CODE_WORKER_EX = 502;

    /** user error, FATAL/ERROR level **/
    public static final int CODE_USER = 700;

    /** default duration seconds is 30min **/
    public static final int DURATION_SECS_DEFAULT = JStormUtils.MIN_10;
    /** queue full error duration seconds is 3min **/
    public static final int DURATION_SECS_QUEUE_FULL = JStormUtils.MIN_1 * 3;
    /** task dead error duration seconds is 3days **/
    public static final int DURATION_SECS_TASK_DEAD = JStormUtils.DAY_1 * 3;
    /** error will never be expired **/
    public static final int DURATION_SECS_FOREVER = Integer.MAX_VALUE;
}
