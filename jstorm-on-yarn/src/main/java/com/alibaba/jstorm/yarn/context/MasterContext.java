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

/**
 * Created by fengjian on 16/4/7.
 */
public interface MasterContext {
    String getUser();

    Credentials getCredentials();

    ApplicationId getApplicationID();

    ApplicationAttemptId getApplicationAttemptId();

    ContainerId getContainerId();

    long getSubmitTime();

    Configuration getConfiguration();

    String getAddress();

    int getRPCPort();

    int getHttpPort();

    Configuration getYarnConfiguration();

    BlockingQueue<Container> getContainers();

    BlockingQueue<Container> getSupervisorContainers();

    BlockingQueue<Container> getNimbusContainers();

    BlockingQueue<AMRMClient.ContainerRequest> getContainerRequest();

    Set<String> getUpgradingContainerIds();

}
