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
        sbCommand.append(classpath).append(JOYConstants.EXECUTOR_CLASS).append(JOYConstants.BLANK);
        sbCommand.append(instanceName).append(JOYConstants.BLANK).append(shellCommand).append(JOYConstants.BLANK);
        sbCommand.append(startType).append(JOYConstants.BLANK).append(containerId).append(JOYConstants.BLANK).append(localDir).append(JOYConstants.BLANK);
        sbCommand.append(deployPath).append(JOYConstants.BLANK).append(hadoopHome).append(JOYConstants.BLANK).append(javaHome).append(JOYConstants.BLANK);
        sbCommand.append(pythonHome).append(JOYConstants.BLANK).append(dstPath).append(JOYConstants.BLANK).append(portList).append(JOYConstants.BLANK);
        sbCommand.append(ExecShellStringPath).append(JOYConstants.BLANK).append(applicationId).append(JOYConstants.BLANK).append(logviewPort).append(JOYConstants.BLANK);
        sbCommand.append(nimbusThriftPort).append(JOYConstants.BLANK).append(shellArgs);
        return sbCommand.toString();
    }
}
