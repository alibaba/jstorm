package com.alibaba.jstorm.yarn.registry;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
import com.alibaba.jstorm.yarn.constants.JstormKeys;
import com.alibaba.jstorm.yarn.utils.JstormYarnUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.*;

/**
 * Created by fengjian on 16/1/14.
 */
public class SlotPortsView {

    private static final Log LOG = LogFactory.getLog(SlotPortsView.class);

    private String instanceName;
    private ContainerId containerId;
    private RegistryOperations registryOperations;
    private int minPort;
    private int maxPort;

    public SlotPortsView(String instanceName, ContainerId containerId, RegistryOperations registryOperations) {
        this.instanceName = instanceName;
        this.containerId = containerId;
        this.registryOperations = registryOperations;
    }

    /**
     * see if port is using by supervisor
     * if used return true, if not, add this port to registry and return false
     *
     * @param supervisorHost
     * @param slotCount
     * @return
     */
    public List<String> getSetPortUsedBySupervisor(String supervisorHost, int slotCount) throws Exception {

        String appPath = RegistryUtils.serviceclassPath(
                JstormKeys.APP_TYPE, "");

        String path = RegistryUtils.serviceclassPath(
                JstormKeys.APP_TYPE, instanceName);

        String containerPath = RegistryUtils.componentPath(
                JstormKeys.APP_TYPE, instanceName, containerId.getApplicationAttemptId().getApplicationId().toString(), containerId.toString());

        List<String> reList = new ArrayList<String>();
        try {

            List<ServiceRecord> hostContainers = new ArrayList<ServiceRecord>();
            Set<String> hostUsedPorts = new HashSet<String>();


            List<String> instanceNames = registryOperations.list(appPath);
            for (String instance : instanceNames) {

                String servicePath = RegistryUtils.serviceclassPath(
                        JstormKeys.APP_TYPE, instance);

                List<String> apps = registryOperations.list(servicePath);

                for (String subapp : apps) {

                    String subAppPath = RegistryUtils.servicePath(
                            JstormKeys.APP_TYPE, instance, subapp);
                    LOG.info("subapp:" + subAppPath);
                    String componentsPath = subAppPath + "/components";
                    if (!registryOperations.exists(componentsPath))
                        continue;
                    Map<String, ServiceRecord> containers = RegistryUtils.listServiceRecords(registryOperations, componentsPath);

                    for (String container : containers.keySet()) {
                        ServiceRecord sr = containers.get(container);
                        LOG.info(sr.toString());

                        if (!sr.get("host").equals(supervisorHost))
                            continue;
                        hostContainers.add(sr);
                        String[] portList = new String[]{};
                        if (sr.get("portList") != null)
                            portList = sr.get("portList").split(",");
                        for (String usedport : portList) {
                            hostUsedPorts.add(usedport);
                        }
                    }
                }
            }

            //scan port range from 9000 to 15000
            for (int i = getMinPort(); i < getMaxPort(); i++) {
                if (JstormYarnUtils.isPortAvailable(supervisorHost, i)) {
                    if (!hostUsedPorts.contains(String.valueOf(i)))
                        reList.add(String.valueOf(i));
                }
                if (reList.size() >= slotCount) {
                    break;
                }
            }


            if (registryOperations.exists(containerPath)) {
                ServiceRecord sr = registryOperations.resolve(containerPath);
                String portListUpdate = JstormYarnUtils.join(reList, ",", false);
                if (sr.get("portList") != null) {
                    String[] portList = sr.get("portList").split(",");
                    portListUpdate = JstormYarnUtils.join(portList, ",", true) + JstormYarnUtils.join(reList, ",", false);
                }
                sr.set("portList", portListUpdate);
                registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
            } else {
                registryOperations.mknode(containerPath, true);
                ServiceRecord sr = new ServiceRecord();
                sr.set("host", supervisorHost);
                String portListUpdate = JstormYarnUtils.join(reList, ",", false);
                sr.set("portList", portListUpdate);
                sr.set(YarnRegistryAttributes.YARN_ID, containerId.toString());
                sr.description = "container";
                sr.set(YarnRegistryAttributes.YARN_PERSISTENCE,
                        PersistencePolicies.CONTAINER);

                registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
            }
            return reList;
        } catch (Exception ex) {
            LOG.error(ex);
            throw ex;
        }
    }

    public String getSupervisorSlotPorts(int memory, int vcores, String supervisorHost) throws Exception {

        String hostPath = RegistryUtils.servicePath(
                JstormKeys.APP_TYPE, this.instanceName, supervisorHost);

        tryHostLock(hostPath);
        try {
            List<String> relist;
            int slotCount = getSlotCount(memory, vcores);

            LOG.info("slotCount:" + slotCount);
            relist = getSetPortUsedBySupervisor(supervisorHost, slotCount);
            LOG.info("get ports string:" + JstormYarnUtils.join(relist, ",", false));

            return JstormYarnUtils.join(relist, ",", false);
        } catch (Exception e) {
            LOG.error(e);
            throw e;
        } finally {
            releaseHostLock(hostPath);
        }
    }

    //todo:  cause we don't support cgroup yet,now vcores is useless
    private int getSlotCount(int memory, int vcores) {
        int cpuports = (int) Math.ceil(vcores / 1.2);
        int memoryports = (int) Math.floor(memory / 4096.0);
//        return cpuports > memoryports ? memoryports : cpuports;
        //  doesn't support cgroup yet
        return memoryports;
    }

    /**
     * see if anyone is updating host's port list, if not start , update this host itself
     * timeout is 45 seconds
     *
     * @param hostPath
     * @throws InterruptedException
     * @throws IOException
     */
    private void tryHostLock(String hostPath) throws Exception {


        //if path has created 60 seconds ago, then delete
        if (registryOperations.exists(hostPath)) {
            try {
                ServiceRecord host = registryOperations.resolve(hostPath);
                Long cTime = Long.parseLong(host.get("cTime", "0"));
                Date now = new Date();
                if (now.getTime() - cTime > JOYConstants.HOST_LOCK_TIMEOUT || cTime > now.getTime())
                    registryOperations.delete(hostPath, true);
            } catch (Exception ex) {
                LOG.error(ex);
//                registryOperations.delete(hostPath, true);
            }
        }

        int failedCount = 45;
        while (!registryOperations.mknode(hostPath, true)) {
            Thread.sleep(1000);
            failedCount--;
            if (failedCount <= 0)
                break;
        }

        if (failedCount > 0) {
            ServiceRecord sr = new ServiceRecord();
            Date date = new Date();
            date.getTime();
            sr.set("cTime", String.valueOf(date.getTime()));
            registryOperations.bind(hostPath, sr, BindFlags.OVERWRITE);
            return;
        }


        throw new Exception("can't get host lock");
    }

    private void releaseHostLock(String hostPath) throws IOException {
        registryOperations.delete(hostPath, true);
    }

    public int getMinPort() {
        return minPort;
    }

    public void setMinPort(int minPort) {
        this.minPort = minPort;
    }

    public int getMaxPort() {
        return maxPort;
    }

    public void setMaxPort(int maxPort) {
        this.maxPort = maxPort;
    }
}
