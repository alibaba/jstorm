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
package com.alibaba.jstorm.yarn.model;


import java.util.Vector;

/**
 * Created by fengjian on 16/4/19.
 */
public class ExecutorMeta {
    private Vector<CharSequence> vargs;
    private String shellCommand;
    private STARTType startType;
    private String runningContainerId;
    private String localDir;
    private String instanceName;
    private String deployPath;
    private String hadoopHome;
    private String javaHome;
    private String pythonHome;
    private String dstPath;
    private String portList;
    private String shellArgs;
    private String ExecShellStringPath;
    private String applicationId;
    private String supervisorLogviewPort;
    private String nimbusThriftPort;

    public ExecutorMeta(String instanceName, String shellCommand, STARTType startType, String runningContainer,
                        String localDir, String deployPath,
                        String hadoopHome, String javaHome, String pythonHome, String dstPath, String portList, String ShellArgs,
                        String ExecShellStringPath, String applicationId, String supervisorLogviewPort, String nimbusThriftPort) {
        this.setInstanceName(instanceName);
        this.setShellCommand(shellCommand);
        this.setStartType(startType);
        this.setRunningContainer(runningContainer);
        this.setLocalDir(localDir);
        this.setDeployPath(deployPath);
        this.setHadoopHome(hadoopHome);
        this.setDeployPath(deployPath);
        this.setHadoopHome(hadoopHome);
        this.setJavaHome(javaHome);
        this.setPythonHome(pythonHome);
        this.setDstPath(dstPath);
        this.setPortList(portList);
        this.setShellArgs(ShellArgs);
        this.setExecShellStringPath(ExecShellStringPath);
        this.setApplicationId(applicationId);
        this.setSupervisorLogviewPort(supervisorLogviewPort);
        this.setNimbusThriftPort(nimbusThriftPort);
    }

    public Vector<CharSequence> getVargs() {
        return vargs;
    }

    public void setVargs(Vector<CharSequence> vargs) {
        this.vargs = vargs;
    }

    public String getShellCommand() {
        return shellCommand;
    }

    public void setShellCommand(String shellCommand) {
        this.shellCommand = shellCommand;
    }

    public STARTType getStartType() {
        return startType;
    }

    public void setStartType(STARTType startType) {
        this.startType = startType;
    }

    public String getRunningContainer() {
        return runningContainerId;
    }

    public void setRunningContainer(String runningContainer) {
        this.runningContainerId = runningContainer;
    }

    public String getLocalDir() {
        return localDir;
    }

    public void setLocalDir(String localDir) {
        this.localDir = localDir;
    }

    public String getDeployPath() {
        return deployPath;
    }

    public void setDeployPath(String deployPath) {
        this.deployPath = deployPath;
    }

    public String getHadoopHome() {
        return hadoopHome;
    }

    public void setHadoopHome(String hadoopHome) {
        this.hadoopHome = hadoopHome;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public String getPythonHome() {
        return pythonHome;
    }

    public void setPythonHome(String pythonHome) {
        this.pythonHome = pythonHome;
    }

    public String getDstPath() {
        return dstPath;
    }

    public void setDstPath(String dstPath) {
        this.dstPath = dstPath;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getPortList() {
        return portList;
    }

    public void setPortList(String portList) {
        this.portList = portList;
    }

    public String getShellArgs() {
        return shellArgs;
    }

    public void setShellArgs(String shellArgs) {
        this.shellArgs = shellArgs;
    }

    public String getExecShellStringPath() {
        return ExecShellStringPath;
    }

    public void setExecShellStringPath(String execShellStringPath) {
        ExecShellStringPath = execShellStringPath;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getSupervisorLogviewPort() {
        return supervisorLogviewPort;
    }

    public void setSupervisorLogviewPort(String supervisorLogviewPort) {
        this.supervisorLogviewPort = supervisorLogviewPort;
    }

    public String getNimbusThriftPort() {
        return nimbusThriftPort;
    }

    public void setNimbusThriftPort(String nimbusThriftPort) {
        this.nimbusThriftPort = nimbusThriftPort;
    }
}
