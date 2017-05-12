package com.alibaba.jstorm.config;

import java.util.Map;

/**
 * @author wange
 * @since 2.1.1
 */
public interface ConfigUpdateHandler {

    void init(Map conf);

    /**
     * update specific config
     *
     * @param conf new conf map to be updated
     */
    void update(Map conf);

    /**
     * update the whole storm.yaml
     *
     * @param jsonConf the storm.yaml stream data
     */
    void updateYaml(String jsonConf);
}
