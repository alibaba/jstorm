package com.alibaba.jstorm.task.master;

public interface TMHandler {
    /**
     * Init TMHandler
     *
     * @param tmContext
     */
    void init(TopologyMasterContext tmContext);

    /**
     * Process Event
     *
     * @param event
     */
    void process(Object event) throws Exception;

    /**
     * Do cleanup job
     * ATTENTION: Due to one TMHandler maybe be register multiple times,
     * So it is likely to be call cleanup multiple times.
     */
    void cleanup();
}
