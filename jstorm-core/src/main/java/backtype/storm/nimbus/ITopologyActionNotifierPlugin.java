package backtype.storm.nimbus;

/**
 * Created by xiaojian.fxj on 2015/11/17.
 */

import java.util.Map;

/**
 * A plugin interface that gets invoked any time there is an action for a topology.
 */
public interface ITopologyActionNotifierPlugin {
    /**
     * Called once during nimbus initialization.
     * @param StormConf
     */
    void prepare(Map StormConf);

    /**
     * When a new actions is executed for a topology, this method will be called.
     * @param topologyName
     * @param action
     */
    void notify(String topologyName, String action);

    /**
     * called during shutdown.
     */
    void cleanup();
}
