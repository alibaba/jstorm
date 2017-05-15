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
package com.alibaba.jstorm.yarn.container;

import com.alibaba.jstorm.yarn.constants.JOYConstants;

/**
 * Created by fengjian on 16/4/21.
 * concat load command string
 */
public class ExecutorLoader {
    public static String loadCommand(String instanceName, String shellCommand, String startType, String containerId, String localDir, String deployPath,
                                     String hadoopHome, String javaHome, String pythonHome, String dstPath, String portList, String shellArgs, String classpath,
                                     String ExecShellStringPath, String applicationId, String logviewPort, String nimbusThriftPort) {
        StringBuffer sbCommand = new StringBuffer();
        sbCommand.append(javaHome).append(JOYConstants.JAVA_CP).append(JOYConstants.BLANK);
        sbCommand.append(classpath).append(JOYConstants.BLANK).append(JOYConstants.EXECUTOR_CLASS).append(JOYConstants.BLANK);
        sbCommand.append(instanceName).append(JOYConstants.BLANK).append(shellCommand).append(JOYConstants.BLANK);
        sbCommand.append(startType).append(JOYConstants.BLANK).append(containerId).append(JOYConstants.BLANK).append(localDir).append(JOYConstants.BLANK);
        sbCommand.append(deployPath).append(JOYConstants.BLANK).append(hadoopHome).append(JOYConstants.BLANK).append(javaHome).append(JOYConstants.BLANK);
        sbCommand.append(pythonHome).append(JOYConstants.BLANK).append(dstPath).append(JOYConstants.BLANK).append(portList).append(JOYConstants.BLANK);
        sbCommand.append(ExecShellStringPath).append(JOYConstants.BLANK).append(applicationId).append(JOYConstants.BLANK).append(logviewPort).append(JOYConstants.BLANK);
        sbCommand.append(nimbusThriftPort).append(JOYConstants.BLANK).append(shellArgs);
        return sbCommand.toString();
    }
}
