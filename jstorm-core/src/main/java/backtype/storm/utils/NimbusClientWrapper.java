package backtype.storm.utils;

import java.util.Map;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormConfig;

import backtype.storm.LocalCluster;
import backtype.storm.generated.Nimbus.Iface;

public class NimbusClientWrapper implements StormObject {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClientWrapper.class);
    private final AtomicBoolean isValid = new AtomicBoolean(true);

    Iface client;
    NimbusClient remoteClient;
    Map conf;
    boolean isLocal = false;

    @Override
    public void init(Map conf) throws Exception {
        this.conf = conf;
        isLocal = StormConfig.try_local_mode(conf);

        if (isLocal) {
            client = LocalCluster.getInstance().getLocalClusterMap().getNimbus();
        } else {
            Map clientConf = Utils.readStormConfig();
            clientConf.putAll(conf);
            remoteClient = NimbusClient.getConfiguredClient(clientConf);
            client = remoteClient.getClient();
        }
        isValid.set(true);
    }

    public void invalidate() {
        isValid.set(false);
    }

    public boolean isValid() {
        return this.isValid.get();
    }

    @Override
    public void cleanup() {
        invalidate();
        if (remoteClient != null) {
            remoteClient.close();
        }
    }

    public Iface getClient() {
        return client;
    }

    public void reconnect() {
        cleanup();
        try {
            init(this.conf);
        } catch (Exception ex) {
            LOG.error("reconnect error, maybe nimbus is not alive.");
        }
    }

}
