/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.oversold;

/**
 * @author fengjian
 * @since 16/11/14
 */
public class GcStrategy {
    private GC gcType;
    private boolean useInitiatingOccupancyOnly;
    private double initiatingOccupancyFraction;
    private int survivorRatio;
    private int maxMetaSpaceSize;

    public GC getGcType() {
        return gcType;
    }

    public void setGcType(GC gcType) {
        this.gcType = gcType;
    }

    public boolean isUseInitiatingOccupancyOnly() {
        return useInitiatingOccupancyOnly;
    }

    public void setUseInitiatingOccupancyOnly(boolean useInitiatingOccupancyOnly) {
        this.useInitiatingOccupancyOnly = useInitiatingOccupancyOnly;
    }

    public double getInitiatingOccupancyFraction() {
        return initiatingOccupancyFraction;
    }

    public void setInitiatingOccupancyFraction(double initiatingOccupancyFraction) {
        this.initiatingOccupancyFraction = initiatingOccupancyFraction;
    }

    public int getSurvivorRatio() {
        return survivorRatio;
    }

    public void setSurvivorRatio(int survivorRatio) {
        this.survivorRatio = survivorRatio;
    }

    public int getMaxMetaSpaceSize() {
        return maxMetaSpaceSize;
    }

    public void setMaxMetaSpaceSize(int maxMetaSpaceSize) {
        this.maxMetaSpaceSize = maxMetaSpaceSize;
    }

    public String toString() {
        StringBuilder sbGcParams = new StringBuilder();
        if (GC.ConcMarkSweepGC == this.gcType) {
            sbGcParams.append(" -XX:+UseParNewGC  -XX:+UseConcMarkSweepGC ");
            sbGcParams.append("-XX:SurvivorRatio=" + this.survivorRatio + "  -XX:+UseCMSInitiatingOccupancyOnly"
                    + "  -XX:CMSFullGCsBeforeCompaction=5 -XX:+HeapDumpOnOutOfMemoryError"
                    + " -XX:+UseCMSCompactAtFullCollection -XX:CMSMaxAbortablePrecleanTime=5000");
            sbGcParams.append("  -XX:CMSInitiatingOccupancyFraction=" + String.valueOf(this.initiatingOccupancyFraction));
        } else if (GC.ParallelGC == this.gcType) {
            sbGcParams.append(" -XX:+UseParallelGC -XX:+UseParallelOldGC ");
        } else if (GC.G1GC == this.gcType) {
            sbGcParams.append(" -XX:+UseG1GC ");
        }
        return "";
    }
}
