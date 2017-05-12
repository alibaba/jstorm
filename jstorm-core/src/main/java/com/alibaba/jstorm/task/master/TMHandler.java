package com.alibaba.jstorm.task.master;

public interface TMHandler {
    /**
     * Init TMHandler
     *
     * @param tmContext topology master context
     */
    void init(TopologyMasterContext tmContext);

    /**
     * Process Event
     *
     * @param event event
     */
    void process(Object event) throws Exception;

    /**
     * Do cleanup job
     * Note that a TMHandler maybe be registered multiple times,
     * it is likely cleanup could be called multiple times.
     */
    void cleanup();
}
