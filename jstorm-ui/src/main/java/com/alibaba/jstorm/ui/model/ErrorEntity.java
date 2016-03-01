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

package com.alibaba.jstorm.ui.model;

import com.alibaba.jstorm.utils.TimeUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ErrorEntity {
    private int errorTime;
    private String error;
    private int errorLapsedSecs;

    public ErrorEntity(int errorTime, String error) {
        this.errorTime = errorTime;
        this.error = error;
        this.errorLapsedSecs = TimeUtils.current_time_secs() - errorTime;
    }

    public int getErrorTime() {
        return errorTime;
    }

    public void setErrorTime(int errorTime) {
        this.errorTime = errorTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getErrorLapsedSecs() {
        return errorLapsedSecs;
    }

    public void setErrorLapsedSecs(int errorLapsedSecs) {
        this.errorLapsedSecs = errorLapsedSecs;
    }
}
