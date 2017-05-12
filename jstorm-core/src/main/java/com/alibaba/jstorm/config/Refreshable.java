package com.alibaba.jstorm.config;

import java.util.Map;

/**
 * @author wange
 * @since 2.1.1
 */
public interface Refreshable {
    void refresh(Map conf);
}
