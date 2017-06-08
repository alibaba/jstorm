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
package backtype.storm.topology;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;

public interface InputDeclarer<T extends InputDeclarer> {
    /**
     * The stream is partitioned by the fields specified in the grouping.
     */
    T fieldsGrouping(String componentId, Fields fields);

    /**
     * The stream is partitioned by the fields specified in the grouping.
     */
    T fieldsGrouping(String componentId, String streamId, Fields fields);

    /**
     * The entire stream goes to a single one of the bolt's tasks. Specifically, it goes to the task with the lowest id.
     */
    T globalGrouping(String componentId);

    /**
     * The entire stream goes to a single one of the bolt's tasks. Specifically, it goes to the task with the lowest id.
     */
    T globalGrouping(String componentId, String streamId);

    /**
     * It's recommended to use localOrShuffleGrouping
     *
     * Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to
     * get an equal number of tuples.
     */
    @Deprecated
    T shuffleGrouping(String componentId);

    /**
     * It's recommended to use localOrShuffleGrouping
     *
     * Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
     */
    @Deprecated
    T shuffleGrouping(String componentId, String streamId);

    /**
     * There are 3 kind of tasks.
     *
     * (1) The tasks run on the same worker process, this kind of tasks have the highest priority.
     * (2) The tasks run on the same node, but not in the same worker, this kind of task have the second  priority.
     * (3) The tasks run on other nodes, this kind of task have the lowest priority
     */
    T localOrShuffleGrouping(String componentId);

    /**
     * There are 3 kind of tasks.
     *
     * (1) The tasks run on the same worker process, this kind of tasks have the highest priority.
     * (2) The tasks run on the same node, but not in the same worker, this kind of task have the second  priority.
     * (3) The tasks run on other nodes, this kind of task have the lowest priority
     */
    T localOrShuffleGrouping(String componentId, String streamId);

    /**
     * There are 2 kind of tasks.
     *
     * (1) The tasks run on the same worker process, this kind of tasks have the highest priority.
     * (2) The tasks run on the other worker process, lower priority
     */
    @Deprecated
    T localFirstGrouping(String componentId);

    @Deprecated
    T localFirstGrouping(String componentId, String streamId);

    /**
     * This grouping specifies that you don't care how the stream is grouped.
     */
    T noneGrouping(String componentId);

    /**
     * This grouping specifies that you don't care how the stream is grouped.
     */
    T noneGrouping(String componentId, String streamId);

    /**
     * The stream is replicated across all the bolt's tasks. Use this grouping with care.
     */
    T allGrouping(String componentId);

    /**
     * The stream is replicated across all the bolt's tasks. Use this grouping with care.
     */
    T allGrouping(String componentId, String streamId);

    /**
     * A stream grouped this way means that the producer of the tuple decides which task of the consumer will receive this tuple.
     */
    T directGrouping(String componentId);

    /**
     * A stream grouped this way means that the producer of the tuple decides which task of the consumer will receive this tuple.
     */
    T directGrouping(String componentId, String streamId);

    /**
     * Tuples are passed to two hashing functions and each target task is decided based on the comparison of the state of candidate nodes.
     *
     * @see https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf
     */
    T partialKeyGrouping(String componentId, Fields fields);

    /**
     * Tuples are passed to two hashing functions and each target task is decided based on the comparison of the state of candidate nodes.
     *
     * @see https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf
     */
    T partialKeyGrouping(String componentId, String streamId, Fields fields);

    /**
     * A custom stream grouping by implementing the CustomStreamGrouping interface.
     */
    T customGrouping(String componentId, CustomStreamGrouping grouping);

    /**
     * A custom stream grouping by implementing the CustomStreamGrouping interface.
     */
    T customGrouping(String componentId, String streamId, CustomStreamGrouping grouping);

    T grouping(GlobalStreamId id, Grouping grouping);

}
