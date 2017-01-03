package com.alibaba.jstorm.yarn.container;

/**
 * Created by fengjian on 16/4/21.
 */
public class ExecutorLoader {
    public static String loadCommand(String instanceName, String shellCommand, String startType, String containerId, String localDir, String deployPath,
                                     String hadoopHome, String javaHome, String pythonHome, String dstPath, String portList, String shellArgs, String classpath,
                                     String ExecShellStringPath, String applicationId, String logviewPort, String nimbusThriftPort) {

//        return "ls && /opt/taobao/java/bin/java -version";
        return javaHome + "/bin/java -cp " + classpath + "  com.alibaba.jstorm.yarn.container.Executor " + instanceName + " " + shellCommand + " " + startType + " "
                + containerId + " "
                + localDir + " " + deployPath + " " + hadoopHome + " " + javaHome + " " + pythonHome + " " + dstPath + " " + portList + " "
                + ExecShellStringPath + " "
                + applicationId + " " + logviewPort + " " + nimbusThriftPort + " " + shellArgs;
//        return "  export CLASSPATH=" + classpath + " &&  java   com.alibaba.jstorm.yarn.container.Executor " + instanceName + " " + shellCommand + " " + startType + " "
//                + containerId + " "
//                + localDir + " " + deployPath + " " + hadoopHome + " " + javaHome + " " + pythonHome + " " + dstPath + " " + portList + " "
//                + ExecShellStringPath + " "
//                + applicationId + " " + shellArgs;
    }
}
