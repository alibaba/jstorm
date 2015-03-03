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
package com.alibaba.jstorm.metric.metrdata;

import java.io.Serializable;


public class MeterData implements Serializable {

	private static final long serialVersionUID = 954627168057659269L;
	
	private long count;
    private double meanRate;
    private double oneMinuteRate;
    private double fiveMinuteRate;
    private double fifteenMinuteRate;
    
    public MeterData() {
    }
    
    public void setCount(long count) {
    	this.count = count;
    }
    
    public long getCount() {
    	return this.count;
    }
    
    public void setMeanRate(double meanRate) {
    	this.meanRate = meanRate;
    }
    
    public double getMeanRate() {
    	return this.meanRate;
    }
    
    public void setOneMinuteRate(double oneMinuteRate) {
    	this.oneMinuteRate = oneMinuteRate;
    }
    
    public double getOneMinuteRate() {
    	return this.oneMinuteRate;
    }
    
    public void setFiveMinuteRate(double fiveMinuteRate) {
    	this.fiveMinuteRate = fiveMinuteRate;
    }
    
    public double getFiveMinuteRate() {
    	return this.fiveMinuteRate;
    }
    
    public void setFifteenMinuteRate(double fifteenMinuteRate) {
    	this.fifteenMinuteRate = fifteenMinuteRate;
    }
    
    public double getFifteenMinuteRate() {
    	return this.fifteenMinuteRate;
    }
}