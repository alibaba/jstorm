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
package com.alibaba.jstorm.transactional.state;

import java.io.Serializable;

public class TransactionState implements Serializable {
    private static final long serialVersionUID = 1124196216381387618L;

    public enum State {
        INIT, ACTIVE, INACTIVE, ROLLBACK
    }

    protected long batchId;
    protected Object systemCheckpoint = null;
    protected Object userCheckpoint = null;

    public TransactionState() {
    }

    public TransactionState(long batchId) {
        this.batchId = batchId;
    }

    public TransactionState(long batchId, Object sysCheckpoint, Object userCheckpoint) {
        this.batchId = batchId;
        this.systemCheckpoint = sysCheckpoint;
        this.userCheckpoint = userCheckpoint;
    }

    public TransactionState(TransactionState state) {
        this.batchId = state.getCurrBatchId();
        this.systemCheckpoint = state.getsysCheckpoint();
        this.userCheckpoint = state.getUserCheckpoint();
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getCurrBatchId() {
        return batchId;
    }

    public Object getsysCheckpoint() {
        return systemCheckpoint;
    }

    public void setSystemCheckpoint(Object checkpoint) {
        systemCheckpoint = checkpoint;
    }

    public Object getUserCheckpoint() {
        return userCheckpoint;
    }

    public void setUserCheckpoint(Object checkpoint) {
        userCheckpoint = checkpoint;
    }

    public void reset() {
        batchId = 0;
        systemCheckpoint = null;
        userCheckpoint = null;
    }

    @Override
    public String toString() {
        return "batchId=" + batchId + ", sysState=" + systemCheckpoint + ", userState=" + userCheckpoint;
    }
}