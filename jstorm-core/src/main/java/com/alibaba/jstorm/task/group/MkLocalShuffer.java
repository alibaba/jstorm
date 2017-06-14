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
package com.alibaba.jstorm.task.group;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MkLocalShuffer extends Shuffer {
    private static final Logger LOG = LoggerFactory.getLogger(MkLocalShuffer.class);

    private List<Integer> outTasks;
    private RandomRange randomrange;
    private Set<Integer> lastLocalNodeTasks;
    private IntervalCheck intervalCheck;
    private WorkerData workerData;
    private boolean isLocal;
    private final List<Integer> allTargetTasks = new ArrayList<>();

    public MkLocalShuffer(List<Integer> workerTasks, List<Integer> allOutTasks, WorkerData workerData) {
        super(workerData);
        List<Integer> localOutTasks = new ArrayList<>();
        allTargetTasks.addAll(allOutTasks);

        for (Integer outTask : allOutTasks) {
            if (workerTasks.contains(outTask)) {
                localOutTasks.add(outTask);
            }
        }
        this.workerData = workerData;
        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60);

        if (localOutTasks.size() != 0) {
            this.outTasks = localOutTasks;
            isLocal = true;
        } else {
            this.outTasks = new ArrayList<>();
            this.outTasks.addAll(allOutTasks);
            refreshLocalNodeTasks();
            isLocal = false;
        }
        randomrange = new RandomRange(outTasks.size());
    }

    /**
     * Don't need to take care of multiple thread racing condition, one thread per task
     */
    private void refreshLocalNodeTasks() {
        Set<Integer> localNodeTasks = workerData.getLocalNodeTasks();

        if (localNodeTasks == null || localNodeTasks.equals(lastLocalNodeTasks)) {
            return;
        }
        LOG.info("Old localNodeTasks:" + lastLocalNodeTasks + ", new:"
                + localNodeTasks);
        lastLocalNodeTasks = localNodeTasks;

        List<Integer> localNodeOutTasks = new ArrayList<>();

        for (Integer outTask : allTargetTasks) {
            if (localNodeTasks.contains(outTask)) {
                localNodeOutTasks.add(outTask);
            }
        }

        if (!localNodeOutTasks.isEmpty()) {
            this.outTasks = localNodeOutTasks;
        }
        randomrange = new RandomRange(outTasks.size());
    }

    public List<Integer> grouper(List<Object> values) {
        if (!isLocal && intervalCheck.check()) {
            refreshLocalNodeTasks();
        }
        int index = getActiveTask(randomrange, outTasks);
        // If none active tasks were found, still send message to a task
        if (index == -1)
            index = randomrange.nextInt();

        return JStormUtils.mk_list(outTasks.get(index));
    }
}
