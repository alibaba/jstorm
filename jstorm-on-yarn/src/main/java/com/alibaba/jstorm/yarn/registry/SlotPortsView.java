package com.alibaba.jstorm.yarn.registry;

import com.alibaba.jstorm.yarn.constants.JOYConstants;
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
                JOYConstants.APP_TYPE, JOYConstants.EMPTY);

        String path = RegistryUtils.serviceclassPath(
                JOYConstants.APP_TYPE, instanceName);

        String containerPath = RegistryUtils.componentPath(
                JOYConstants.APP_TYPE, instanceName, containerId.getApplicationAttemptId().getApplicationId().toString(), containerId.toString());

        List<String> reList = new ArrayList<String>();
        try {

            List<ServiceRecord> hostContainers = new ArrayList<ServiceRecord>();
            Set<String> hostUsedPorts = new HashSet<String>();


            List<String> instanceNames = registryOperations.list(appPath);
            for (String instance : instanceNames) {

                String servicePath = RegistryUtils.serviceclassPath(
                        JOYConstants.APP_TYPE, instance);

                List<String> apps = registryOperations.list(servicePath);

                for (String subapp : apps) {

                    String subAppPath = RegistryUtils.servicePath(
                            JOYConstants.APP_TYPE, instance, subapp);
                    String componentsPath = subAppPath + JOYConstants.COMPONENTS;
                    if (!registryOperations.exists(componentsPath))
                        continue;
                    Map<String, ServiceRecord> containers = RegistryUtils.listServiceRecords(registryOperations, componentsPath);

                    for (String container : containers.keySet()) {
                        ServiceRecord sr = containers.get(container);
                        LOG.info(sr.toString());

                        if (!sr.get(JOYConstants.HOST).equals(supervisorHost))
                            continue;
                        hostContainers.add(sr);
                        String[] portList = new String[]{};
                        if (sr.get(JOYConstants.PORT_LIST) != null)
                            portList = sr.get(JOYConstants.PORT_LIST).split(JOYConstants.COMMA);
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
                String portListUpdate = JstormYarnUtils.join(reList, JOYConstants.COMMA, false);
                if (sr.get(JOYConstants.PORT_LIST) != null) {
                    String[] portList = sr.get(JOYConstants.PORT_LIST).split(JOYConstants.COMMA);
                    portListUpdate = JstormYarnUtils.join(portList, JOYConstants.COMMA, true) + JstormYarnUtils.join(reList, JOYConstants.COMMA, false);
                }
                sr.set(JOYConstants.PORT_LIST, portListUpdate);
                registryOperations.bind(containerPath, sr, BindFlags.OVERWRITE);
            } else {
                registryOperations.mknode(containerPath, true);
                ServiceRecord sr = new ServiceRecord();
                sr.set(JOYConstants.HOST, supervisorHost);
                String portListUpdate = JstormYarnUtils.join(reList, JOYConstants.COMMA, false);
                sr.set(JOYConstants.PORT_LIST, portListUpdate);
                sr.set(YarnRegistryAttributes.YARN_ID, containerId.toString());
                sr.description = JOYConstants.CONTAINER;
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
                JOYConstants.APP_TYPE, this.instanceName, supervisorHost);

        tryHostLock(hostPath);
        try {
            List<String> relist;
            int slotCount = getSlotCount(memory, vcores);

            LOG.info("slotCount:" + slotCount);
            relist = getSetPortUsedBySupervisor(supervisorHost, slotCount);
            LOG.info("get ports string:" + JstormYarnUtils.join(relist, JOYConstants.COMMA, false));

            return JstormYarnUtils.join(relist, JOYConstants.COMMA, false);
        } catch (Exception e) {
            LOG.error(e);
            throw e;
        } finally {
            releaseHostLock(hostPath);
        }
    }

    //todo:  cause we don't support cgroup yet,now vcores is useless
    private int getSlotCount(int memory, int vcores) {
        int cpuports = (int) Math.ceil(vcores / JOYConstants.JSTORM_VCORE_WEIGHT);
        int memoryports = (int) Math.floor(memory / JOYConstants.JSTORM_MEMORY_WEIGHT);
//        return cpuports > memoryports ? memoryports : cpuports;
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
                Long cTime = Long.parseLong(host.get(JOYConstants.CTIME, JOYConstants.DEFAULT_CTIME));
                Date now = new Date();
                if (now.getTime() - cTime > JOYConstants.HOST_LOCK_TIMEOUT || cTime > now.getTime())
                    registryOperations.delete(hostPath, true);
            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

        int failedCount = JOYConstants.RETRY_TIMES;
        while (!registryOperations.mknode(hostPath, true)) {
            Thread.sleep(JOYConstants.SLEEP_INTERVAL);
            failedCount--;
            if (failedCount <= 0)
                break;
        }

        if (failedCount > 0) {
            ServiceRecord sr = new ServiceRecord();
            Date date = new Date();
            date.getTime();
            sr.set(JOYConstants.CTIME, String.valueOf(date.getTime()));
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
