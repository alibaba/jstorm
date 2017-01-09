package com.alibaba.jstorm.yarn.registry;

import com.alibaba.jstorm.yarn.constants.JstormKeys;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * every container has a view which indicate used resources include core memory,port etc
 *
 * Created by fengjian on 16/3/29.
 */
public class ContainersView {
    private static final Log LOG = LogFactory.getLog(ContainersView.class);

    private String instanceName;
    private String host;
    private ContainerId containerId;
    private RegistryOperations registryOperations;

    public ContainersView(String instanceName, String host, ContainerId containerId, RegistryOperations registryOperations) {
        this.instanceName = instanceName;
        this.containerId = containerId;
        this.registryOperations = registryOperations;
        this.host = host;
    }

    public List<String> getContainerFreePorts() {
        return null;
    }

    public List<String> getHostUsedPort() {
        return null;
    }

    public List<String> findFreePorts(int count) {
        return null;
    }

    public String getHostPath() {
        String hostPath = RegistryUtils.servicePath(
                JstormKeys.APP_TYPE, this.instanceName, host);
        return hostPath;
    }

    /**
     * see if anyone is updating host's port list, if not start , update this host itself
     * timeout is 45 seconds
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void tryHostLock() throws Exception {

        String hostPath = getHostPath();
        //if path has created 60 seconds ago, then delete
        if (registryOperations.exists(hostPath)) {
            try {
                ServiceRecord host = registryOperations.resolve(hostPath);
                Long cTime = Long.parseLong(host.get("cTime", "0"));
                Date now = new Date();
                if (now.getTime() - cTime > 60 * 1000 || cTime > now.getTime())
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

    private void releaseHostLock() throws IOException {
        String hostPath = getHostPath();
        registryOperations.delete(hostPath, true);
    }


}
