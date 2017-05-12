package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.blobstore.BlobStore;
import com.alibaba.jstorm.blobstore.BlobStoreUtils;
import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.task.upgrade.GrayUpgradeConfig;
import com.google.common.collect.Sets;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wange
 * @since 24/02/2017
 */
public class RollbackTransitionCallback extends BaseCallback {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private NimbusData data;
    private String topologyId;

    public RollbackTransitionCallback(NimbusData data, String topologyId) {
        this.data = data;
        this.topologyId = topologyId;
    }

    @Override
    public <T> Object execute(T... args) {
        LOG.warn("Some workers are dead during upgrade, upgrade fails, rolling back...");
        try {
            StormBase stormBase = data.getStormClusterState().storm_base(topologyId, null);
            if (stormBase.getStatus().getStatusType() == StatusType.rollback) {
                LOG.warn("Topology {} is already rolling back, skip.", topologyId);
                return null;
            }

            StormClusterState stormClusterState = data.getStormClusterState();
            BlobStore blobStore = data.getBlobStore();
            GrayUpgradeConfig upgradeConfig = (GrayUpgradeConfig) stormClusterState.get_gray_upgrade_conf(topologyId);
            if (upgradeConfig != null) {
                String codeKeyBak = StormConfig.master_stormcode_bak_key(topologyId);
                LOG.info("Restoring storm code with key:{}", codeKeyBak);
                BlobStoreUtils.updateBlob(blobStore, StormConfig.master_stormcode_key(topologyId),
                        blobStore.getBlob(codeKeyBak));

                String stormJarKeyBak = StormConfig.master_stormjar_bak_key(topologyId);
                LOG.info("Restoring storm jar with key:{}", stormJarKeyBak);
                BlobStoreUtils.updateBlob(blobStore, StormConfig.master_stormjar_key(topologyId),
                        blobStore.getBlob(stormJarKeyBak));

                upgradeConfig.setRollback(true);
                stormClusterState.set_gray_upgrade_conf(topologyId, upgradeConfig);

                Set<String> upgradedWorkers = Sets.newHashSet(stormClusterState.get_upgraded_workers(topologyId));
                if (upgradedWorkers.size() > 0) {
                    LOG.info("Setting rollback workers:{}", upgradedWorkers);
                    for (String upgradedWorker : upgradedWorkers) {
                        stormClusterState.add_upgrading_worker(topologyId, upgradedWorker);
                    }
                }
            } else {
                LOG.warn("Cannot get upgrade config, aborting...");
            }

            return new StormStatus(StatusType.rollback);
        } catch (Exception ex) {
            LOG.error("Failed to call RollbackTransitionCallback!", ex);
        }
        return null;
    }
}
