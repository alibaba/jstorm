package com.alibaba.jstorm.yarn.handler;

import com.alibaba.jstorm.yarn.appmaster.JstormMaster;
import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.context.JstormMasterContext;
import com.alibaba.jstorm.yarn.generated.JstormAM;
import com.alibaba.jstorm.yarn.utils.JstormYarnUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by fengjian on 15/12/22.
 */
public class JstormAMHandler implements JstormAM.Iface {

    public JstormAMHandler(JstormMaster jm) {
        amRMClient = jstormMasterContext.amRMClient;
        requestQueue = jstormMasterContext.requestBlockingQueue;
        this.jstormMasterContext = jm.jstormMasterContext;
        this.jstormMaster = jm;
    }

    private static final Log LOG = LogFactory.getLog(JstormAMHandler.class);
    private AMRMClientAsync amRMClient;
    private BlockingQueue requestQueue;
    private JstormMasterContext jstormMasterContext;
    private JstormMaster jstormMaster;

    @Override
    public void addSupervisors(int number, int containerMemory, int containerVcores) throws TException {
        if (containerMemory > jstormMasterContext.maxMemory) {
            containerMemory = jstormMasterContext.maxMemory;
        }
        if (containerVcores > jstormMasterContext.maxVcores) {
            containerVcores = jstormMasterContext.maxVcores;
        }
        for (int i = 0; i < number; i++) {
            //set priority to 0 which identity this container is allocated for supervisor
            AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(containerMemory, containerVcores, 0, "*");
            try {
                requestQueue.put(containerAsk);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            amRMClient.addContainerRequest(containerAsk);
            jstormMasterContext.numRequestedContainers.getAndIncrement();
        }
    }

    @Override
    public void addSpecSupervisor(int number, int container_memory, int container_vcorecount, List<String> racks, List<String> hosts) throws TException {

        LOG.info("number:" + number + "; memory:" + container_memory + "; vcore:" + container_vcorecount + "; racks:" + JstormYarnUtils.join(racks, ",", false));
        LOG.info("hosts:" + JstormYarnUtils.join(hosts, ",", false));

        if (container_memory > jstormMasterContext.maxMemory) {
            container_vcorecount = jstormMasterContext.maxMemory;
        }
        if (container_vcorecount > jstormMasterContext.maxVcores) {
            container_vcorecount = jstormMasterContext.maxVcores;
        }
        for (int i = 0; i < number; i++) {
            AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(container_memory, container_vcorecount, 0, racks.toArray(new String[0]), hosts.toArray(new String[0]));
            try {
                requestQueue.put(containerAsk);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            amRMClient.addContainerRequest(containerAsk);
            jstormMasterContext.numRequestedContainers.getAndIncrement();
        }
    }


    @Override
    public String info() throws TException {
        StringBuffer sbRet = new StringBuffer();
        sbRet.append("JstormOnYarn\n");
        sbRet.append("Instance Name:" + jstormMasterContext.instanceName + "\n");
        sbRet.append("Jstorm's location on hdfs:" + jstormMasterContext.deployPath + "\n");
        if (jstormMasterContext.user != null) {
            sbRet.append("Jstorm's data path:" + jstormMasterContext.nimbusDataDirPrefix + jstormMasterContext.instanceName + "\n");
            sbRet.append("Cluster userName:" + jstormMasterContext.user + "\n");
        }
        sbRet.append("Nimbus Count:" + jstormMasterContext.nimbusContainers.size() + "\n");
        sbRet.append("Supervisor Count:" + jstormMasterContext.supervisorContainers.size() + "\n");
        sbRet.append("detail :\n");
        sbRet.append("Type      \tContainerId                              \tHost      \tContainerMemory\tContainerVCores\n");
        for (Container container : jstormMasterContext.nimbusContainers) {
            sbRet.append("Nimbus    \t" + container.getId().toString() + "\t" + container.getNodeId().getHost() + "\t" + container.getResource().getMemory()
                    + "\t        " + container.getResource().getVirtualCores() + "\n");
        }
        for (Container container : jstormMasterContext.supervisorContainers) {
            sbRet.append("Supervisor\t" + container.getId().toString() + "\t" + container.getNodeId().getHost() + "\t" + container.getResource().getMemory()
                    + "\t        " + container.getResource().getVirtualCores() + "\n");
        }
        LOG.info("info is: " + sbRet.toString());
        return sbRet.toString();
    }

    @Override
    public void stopAppMaster() throws TException {
        stopNimbus();
        stopSupervisors();
        jstormMaster.killApplicationMaster();
    }

    @Override
    public void startSpecNimbus(int number, int containerMemory, int containerVirtualCores, List<String> racks, List<String> hosts) throws TException {
        //set priority to 1 which identity this container is allocated for nimbus
        String dstHost = "*";
        if (!jstormMasterContext.previousNimbusHost.equals("") && jstormMasterContext.nimbusContainers.size() == 0)
            dstHost = jstormMasterContext.previousNimbusHost;

        LOG.info("dstHost:" + dstHost);
        AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(containerMemory, containerVirtualCores, 1,
                racks.toArray(new String[0]), hosts.toArray(new String[0]));
        try {
            requestQueue.put(containerAsk);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        amRMClient.addContainerRequest(containerAsk);
        jstormMasterContext.numRequestedContainers.getAndIncrement();
    }

    @Override
    public void startNimbus(int number, int containerMemory, int containerVirtualCores) throws TException {
        //set priority to 1 which identity this container is allocated for nimbus
        String dstHost = "*";
        if (!jstormMasterContext.previousNimbusHost.equals("") && jstormMasterContext.nimbusContainers.size() == 0)
            dstHost = jstormMasterContext.previousNimbusHost;

        LOG.info("dstHost:" + dstHost);
        AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(containerMemory, containerVirtualCores, 1, dstHost);
        try {
            requestQueue.put(containerAsk);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        amRMClient.addContainerRequest(containerAsk);
        jstormMasterContext.numRequestedContainers.getAndIncrement();
    }

    @Override
    public void stopNimbus() throws TException {
        if (jstormMasterContext.nimbusContainers.isEmpty())
            return;
        int nimbusCount = jstormMasterContext.nimbusContainers.size();
        for (int i = 0; i < nimbusCount; i++) {
            Container container = jstormMasterContext.nimbusContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release nimbus container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeNimbus(int number) throws TException {
        if (jstormMasterContext.nimbusContainers.isEmpty())
            return;
        for (int i = 0; i < number; i++) {
            Container container = jstormMasterContext.nimbusContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release nimbus container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSpecNimbus(String container_id) throws TException {
        if (jstormMasterContext.nimbusContainers.isEmpty())
            return;
        for (Container container : jstormMasterContext.nimbusContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                jstormMasterContext.nimbusContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
            }
        }
    }

    @Override
    public void startSupervisors() throws TException {

    }

    @Override
    public void stopSupervisors() throws TException {
        if (jstormMaster.jstormMasterContext.supervisorContainers.isEmpty())
            return;
        int supervisorCount = jstormMaster.jstormMasterContext.supervisorContainers.size();
        for (int i = 0; i < supervisorCount; i++) {
            Container container = jstormMaster.jstormMasterContext.supervisorContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release all supervisor container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSupervisors(int number) throws TException {
        if (jstormMaster.jstormMasterContext.supervisorContainers.isEmpty())
            return;
        for (int i = 0; i < number; i++) {
            Container container = jstormMaster.jstormMasterContext.supervisorContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release supervisor's " + String.valueOf(number) + " container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSpecSupervisors(String container_id) throws TException {
        LOG.info("remove spec supervisor: " + container_id);
        for (Container container : jstormMaster.jstormMasterContext.supervisorContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                LOG.info("found one  remove");
                jstormMaster.jstormMasterContext.supervisorContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
                return;
            }
        }

        LOG.info("remove spec nimbus : " + container_id);
        for (Container container : jstormMaster.jstormMasterContext.nimbusContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                LOG.info("found one  remove");
                jstormMaster.jstormMasterContext.nimbusContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
            }
        }
    }

    @Override
    public void upgradeCluster() throws TException {

        for (Container supervisorContainer : jstormMaster.jstormMasterContext.supervisorContainers) {
            //upgrade supervisor
            String containerPath = RegistryUtils.componentPath(
                    JOYConstants.APP_TYPE, jstormMasterContext.instanceName,
                    supervisorContainer.getId().getApplicationAttemptId().getApplicationId().toString(),
                    supervisorContainer.getId().toString());
            try {
                if (jstormMaster.registryOperations.exists(containerPath)) {
                    ServiceRecord sr = jstormMaster.registryOperations.resolve(containerPath);
                    sr.set("needUpgrade", "true");
                    jstormMaster.registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for (Container nimbusContainer : jstormMaster.jstormMasterContext.nimbusContainers) {
            //upgrade nimbus
            String containerPath = RegistryUtils.componentPath(
                    JOYConstants.APP_TYPE, jstormMasterContext.instanceName,
                    nimbusContainer.getId().getApplicationAttemptId().getApplicationId().toString(),
                    nimbusContainer.getId().toString());

            try {
                if (jstormMaster.registryOperations.exists(containerPath)) {
                    ServiceRecord sr = jstormMaster.registryOperations.resolve(containerPath);
                    sr.set("needUpgrade", "true");
                    jstormMaster.registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void shutdown() throws TException {

    }

    @Override
    public String getConfig() throws TException {

        StringBuffer sbRet = new StringBuffer();
        sbRet.append("Jstorm's location on hdfs:" + jstormMasterContext.deployPath + "\n");
        if (jstormMasterContext.nimbusDataDirPrefix != null)
            sbRet.append("Jstorm's data path:" + jstormMasterContext.nimbusDataDirPrefix + jstormMasterContext.instanceName + "\n");
        if (jstormMasterContext.user != null) {
            sbRet.append("Cluster username:" + jstormMasterContext.user + "\n");
            sbRet.append("Cluster password:" + "***" + jstormMasterContext.password.substring(3, jstormMasterContext.password.length() - 2) + "**" + "\n");
            sbRet.append("Cluster oldpassword:" + "***" + jstormMasterContext.oldPassword.substring(3, jstormMasterContext.oldPassword.length() - 2) + "**" + "\n");
        }
        return sbRet.toString();
    }

    @Override
    public void setConfig(String key, String value) throws TException {
        if (key.equals("password")) {
            jstormMasterContext.password = value;
        } else if (key.equals("oldpassword")) {
            jstormMasterContext.oldPassword = value;
        } else if (key.equals("username")) {
            jstormMasterContext.user = value;
        }
    }
}
