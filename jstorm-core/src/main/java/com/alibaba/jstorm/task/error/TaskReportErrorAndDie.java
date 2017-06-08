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
package com.alibaba.jstorm.task.error;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * report task error to ZK and halt the process
 *
 * @author yannian
 */
public class TaskReportErrorAndDie implements ITaskReportErr {
    private ITaskReportErr reportError;
    private RunnableCallback haltFn;

    public TaskReportErrorAndDie(ITaskReportErr reportError, RunnableCallback haltFn) {
        this.reportError = reportError;
        this.haltFn = haltFn;
    }

    // If a throwable was caught, the error will be reported to zk and current task will be shutdown.
    @Override
    public void report(Throwable error) {
        this.reportError.report(error);
        this.haltFn.run();
    }

    @Override
    public void report(String error) {
        this.reportError.report(error);
    }

    @Override
    public void report(String error, String errorLevel, int errorCode, int duration) {
        this.reportError.report(error, errorLevel, errorCode, duration);
    }
}
