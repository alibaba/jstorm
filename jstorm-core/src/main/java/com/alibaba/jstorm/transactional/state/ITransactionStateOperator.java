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

public interface ITransactionStateOperator extends Serializable {
    void initState(Object userState);

    /**
     * Called when current batch is finished
     *
     * @return user state to be committed
     */
    Object finishBatch(long batchId);

    /**
     * @return snapshot state which is used to retrieve the persistent user state
     */
    Object commit(long batchId, Object state);

    /**
     * @param userState user state for rollback
     */
    void rollBack(Object userState);

    /**
     * Called when the whole topology finishes committing
     */
    void ackCommit(long batchId, long timeStamp);
}