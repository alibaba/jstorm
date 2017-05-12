package com.alibaba.jstorm.oversold;

import com.alibaba.jstorm.client.ConfigExtension;

import java.util.Map;

/**
 * @author fengjian
 * @since 16/11/14
 */
public class GcStrategyManager {


    public static GcStrategy getGcStrategy(Map conf) {
        double memWeight = ConfigExtension.getSupervisorSlotsPortMemWeight(conf);
        if (memWeight >= 1) memWeight = 1;
        else if (memWeight <= 0.5) memWeight = 0.5;
        GcStrategy gcStrategy = new GcStrategy();
        gcStrategy.setGcType(GC.ConcMarkSweepGC);
        gcStrategy.setInitiatingOccupancyFraction(memWeight * 0.68);
        gcStrategy.setUseInitiatingOccupancyOnly(true);
        gcStrategy.setSurvivorRatio(4);
        return gcStrategy;
    }
}
