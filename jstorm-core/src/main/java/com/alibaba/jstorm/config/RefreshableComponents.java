package com.alibaba.jstorm.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author wange
 * @since 2.1.1
 */
public class RefreshableComponents {
    private static final List<Refreshable> refreshableList = new ArrayList<>();

    public static void registerRefreshable(Refreshable refreshable) {
        refreshableList.add(refreshable);
    }

    public static void refresh(Map conf) {
        for (Refreshable refreshable : refreshableList) {
            refreshable.refresh(conf);
        }
    }
}
