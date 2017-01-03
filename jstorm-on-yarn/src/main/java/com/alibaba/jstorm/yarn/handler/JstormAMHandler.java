package com.alibaba.jstorm.yarn.handler;

import com.alibaba.jstorm.yarn.appmaster.JstormMaster;
import com.alibaba.jstorm.yarn.constants.JstormKeys;
import com.alibaba.jstorm.yarn.generated.JstormAM;
import com.alibaba.jstorm.yarn.utils.JstormYarnUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.thrift.TException;

/**
 * @author fengjian
 */
public class JstormAMHandler implements JstormAM.Iface {

    public JstormAMHandler(JstormMaster jm) {
        amRMClient = jm.amRMClient;
        requestQueue = jm.requestBlockingQueue;
        jstormMaster = jm;
    }

    private static final Log LOG = LogFactory.getLog(JstormAMHandler.class);
    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private BlockingQueue<AMRMClient.ContainerRequest> requestQueue;
    private JstormMaster jstormMaster;


    @Override
    public void addSupervisors(int number, int containerMemory, int containerVcores) throws TException {
        if (containerMemory > jstormMaster.maxMemory) {
            containerMemory = jstormMaster.maxMemory;
        }
        if (containerVcores > jstormMaster.maxVcores) {
            containerVcores = jstormMaster.maxVcores;
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
            jstormMaster.numRequestedContainers.getAndIncrement();
        }
    }

    @Override
    public void addSpecSupervisor(int number, int container_memory, int container_vcorecount,
                                  List<String> racks, List<String> hosts) throws TException {

        LOG.info("nuber:" + number);
        LOG.info("memory:" + container_memory);
        LOG.info("vcore:" + container_vcorecount);
        LOG.info("racks:" + JstormYarnUtils.join(racks, ",", false));
        LOG.info("hosts:" + JstormYarnUtils.join(hosts, ",", false));

        if (container_memory > jstormMaster.maxMemory) {
            container_vcorecount = jstormMaster.maxMemory;
        }
        if (container_vcorecount > jstormMaster.maxVcores) {
            container_vcorecount = jstormMaster.maxVcores;
        }
        for (int i = 0; i < number; i++) {
            AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(
                    container_memory, container_vcorecount, 0, racks.toArray(new String[0]), hosts.toArray(new String[0]));
            try {
                requestQueue.put(containerAsk);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            amRMClient.addContainerRequest(containerAsk);
            jstormMaster.numRequestedContainers.getAndIncrement();
        }
    }


    @Override
    public String info() throws TException {
        StringBuilder sbRet = new StringBuilder();
        sbRet.append("JstormOnYarn\n");
        sbRet.append("Instance Name:" + jstormMaster.instanceName + "\n");
        sbRet.append("Jstorm's location on hdfs:" + jstormMaster.deployPath + "\n");
        if (jstormMaster.user != null) {
            sbRet.append("Jstorm's data path:" + jstormMaster.nimbusDataDirPrefix + jstormMaster.instanceName + "\n");
            sbRet.append("Cluster userName:" + jstormMaster.user + "\n");
        }
        sbRet.append("Nimbus Count:" + jstormMaster.nimbusContainers.size() + "\n");
        sbRet.append("Supervisor Count:" + jstormMaster.supervisorContainers.size() + "\n");
        sbRet.append("detail :\n");
        sbRet.append("Type      \tContainerId                              \tHost      \tContainerMemory\tContainerVCores\n");
        for (Container container : jstormMaster.nimbusContainers) {
            sbRet.append("Nimbus    \t" + container.getId().toString() + "\t" + container.getNodeId().getHost() + "\t"
                    + container.getResource().getMemory()
                    + "\t        " + container.getResource().getVirtualCores() + "\n");
        }
        for (Container container : jstormMaster.supervisorContainers) {
            sbRet.append("Supervisor\t" + container.getId().toString() + "\t" + container.getNodeId().getHost() + "\t"
                    + container.getResource().getMemory()
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
        if (!jstormMaster.previousNimbusHost.equals("") && jstormMaster.nimbusContainers.size() == 0)
            dstHost = jstormMaster.previousNimbusHost;

        LOG.info("dstHost:" + dstHost);
        AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(containerMemory, containerVirtualCores, 1,
                racks.toArray(new String[0]), hosts.toArray(new String[0]));
        try {
            requestQueue.put(containerAsk);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        amRMClient.addContainerRequest(containerAsk);
        jstormMaster.numRequestedContainers.getAndIncrement();
    }

    @Override
    public void startNimbus(int number, int containerMemory, int containerVirtualCores) throws TException {
        //set priority to 1 which identity this container is allocated for nimbus
        String dstHost = "*";
        if (!jstormMaster.previousNimbusHost.equals("") && jstormMaster.nimbusContainers.size() == 0)
            dstHost = jstormMaster.previousNimbusHost;

        LOG.info("dstHost:" + dstHost);
        AMRMClient.ContainerRequest containerAsk = jstormMaster.setupContainerAskForRM(containerMemory, containerVirtualCores, 1, dstHost);
        try {
            requestQueue.put(containerAsk);
        } catch (InterruptedException ignored) {
        }
        amRMClient.addContainerRequest(containerAsk);
        jstormMaster.numRequestedContainers.getAndIncrement();
    }

    @Override
    public void stopNimbus() throws TException {
        if (jstormMaster.nimbusContainers.isEmpty())
            return;
        int nimbusCount = jstormMaster.nimbusContainers.size();
        for (int i = 0; i < nimbusCount; i++) {
            Container container = jstormMaster.nimbusContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release nimbus container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeNimbus(int number) throws TException {
        if (jstormMaster.nimbusContainers.isEmpty())
            return;
        for (int i = 0; i < number; i++) {
            Container container = jstormMaster.nimbusContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release nimbus container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSpecNimbus(String container_id) throws TException {
        if (jstormMaster.nimbusContainers.isEmpty())
            return;
        for (Container container : jstormMaster.nimbusContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                jstormMaster.nimbusContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
            }
        }
    }

    @Override
    public void startSupervisors() throws TException {

    }

    @Override
    public void stopSupervisors() throws TException {
        if (jstormMaster.supervisorContainers.isEmpty())
            return;
        int supervisorCount = jstormMaster.supervisorContainers.size();
        for (int i = 0; i < supervisorCount; i++) {
            Container container = jstormMaster.supervisorContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release all supervisor container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSupervisors(int number) throws TException {
        if (jstormMaster.supervisorContainers.isEmpty())
            return;
        for (int i = 0; i < number; i++) {
            Container container = jstormMaster.supervisorContainers.poll();
            if (container != null) {
                amRMClient.releaseAssignedContainer(container.getId());
                LOG.info("release supervisor's " + String.valueOf(number) + " container, id: " + container.getId().toString());
            }
        }
    }

    @Override
    public void removeSpecSupervisors(String container_id) throws TException {
        LOG.info("remove spec supervisor: " + container_id);
        for (Container container : jstormMaster.supervisorContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                LOG.info("found one  remove");
                jstormMaster.supervisorContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
                return;
            }
        }

        LOG.info("remove spec nimbus : " + container_id);
        for (Container container : jstormMaster.nimbusContainers) {
            if (container.getId().toString().trim().equals(container_id.trim())) {
                LOG.info("found one  remove");
                jstormMaster.nimbusContainers.remove(container);
                amRMClient.releaseAssignedContainer(container.getId());
            }
        }
    }

    @Override
    public void upgradeCluster() throws TException {

        for (Container supervisorContainer : jstormMaster.supervisorContainers) {
            //upgrade supervisor
            String containerPath = RegistryUtils.componentPath(
                    JstormKeys.APP_TYPE, jstormMaster.instanceName,
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

        for (Container nimbusContainer : jstormMaster.nimbusContainers) {
            //upgrade nimbus
            String containerPath = RegistryUtils.componentPath(
                    JstormKeys.APP_TYPE, jstormMaster.instanceName,
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
        StringBuilder sbRet = new StringBuilder();
        sbRet.append("Jstorm's location on hdfs:" + jstormMaster.deployPath + "\n");
        if (jstormMaster.nimbusDataDirPrefix != null)
            sbRet.append("Jstorm's data path:" + jstormMaster.nimbusDataDirPrefix + jstormMaster.instanceName + "\n");
        if (jstormMaster.user != null) {
            sbRet.append("Cluster username:" + jstormMaster.user + "\n");
            sbRet.append("Cluster password:" + "***" + jstormMaster.password.substring(3, jstormMaster.password.length() - 2) + "**" + "\n");
            sbRet.append("Cluster oldpassword:" + "***" + jstormMaster.oldPassword.substring(3, jstormMaster.oldPassword.length() - 2) + "**" + "\n");
        }
        return sbRet.toString();
    }

    @Override
    public void setConfig(String key, String value) throws TException {
        if (key.equals("password")) {
            jstormMaster.password = value;
        } else if (key.equals("oldpassword")) {
            jstormMaster.oldPassword = value;
        } else if (key.equals("username")) {
            jstormMaster.user = value;
        }
    }
}
