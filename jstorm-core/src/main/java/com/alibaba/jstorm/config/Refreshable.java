package com.alibaba.jstorm.config;

import java.util.Map;

/**
 * @author wange
 * @since 16/5/9
 */
public interface Refreshable {
    void refresh(Map conf);
}
