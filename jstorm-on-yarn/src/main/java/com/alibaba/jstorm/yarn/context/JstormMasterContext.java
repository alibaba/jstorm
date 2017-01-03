package com.alibaba.jstorm.yarn.context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by fengjian on 16/4/7.
 */
public class JstormMasterContext implements MasterContext {

    private String user;
    private ContainerId containerId;
    private ApplicationAttemptId attemptId;
    private Credentials credentials;
    private Configuration config;
    private long submitTime;
    private String address;
    private int rpcPort;
    private int httpPort;
    private BlockingQueue<AMRMClient.ContainerRequest> requestBlockingQueue;
    private BlockingQueue<Container> nimbusContainers = new LinkedBlockingQueue<Container>();
    private BlockingQueue<Container> supervisorContainers = new LinkedBlockingQueue<Container>();

    public JstormMasterContext(String user, ContainerId containerId,
                               ApplicationAttemptId applicationAttemptId,
                               long appSubmitTime, String nodeHostString,
                               Configuration yarnConfig) {
        this.user = user;
        this.containerId = containerId;
        this.attemptId = applicationAttemptId;
        this.credentials = new Credentials();
        this.submitTime = appSubmitTime;
        this.address = nodeHostString;
        this.config = yarnConfig;
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Override
    public Credentials getCredentials() {
        return this.credentials;
    }

    @Override
    public ApplicationId getApplicationID() {
        return this.attemptId.getApplicationId();
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
        return this.attemptId;
    }

    @Override
    public ContainerId getContainerId() {
        return this.containerId;
    }

    @Override
    public long getSubmitTime() {
        return this.submitTime;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public int getRPCPort() {
        return this.rpcPort;
    }

    @Override
    public int getHttpPort() {
        return this.httpPort;
    }

    @Override
    public Configuration getYarnConfiguration() {
        return this.config;
    }

    @Override
    public BlockingQueue<Container> getContainers() {
        return null;
    }

    @Override
    public BlockingQueue<Container> getSupervisorContainers() {
        return this.supervisorContainers;
    }

    @Override
    public BlockingQueue<Container> getNimbusContainers() {
        return this.nimbusContainers;
    }

    @Override
    public BlockingQueue<AMRMClient.ContainerRequest> getContainerRequest() {
        return null;
    }

    @Override
    public Set<String> getUpgradingContainerIds() {
        return null;
    }
}
