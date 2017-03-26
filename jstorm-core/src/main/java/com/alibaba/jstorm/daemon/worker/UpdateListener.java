/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.jstorm.daemon.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UpdateListener is used to dynamic update configurations in worker
 * triggered by update topology action
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UpdateListener {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateListener.class);

    interface IUpdater {
        void update(Map conf);
    }

    private List<IUpdater> updaters = new ArrayList<>();

    /**
     * register an updater which implement IUpdate
     * @param updater used to update configurations in worker
     */
    public void registerUpdater(IUpdater updater) {
        updaters.add(updater);
    }

    /**
     * trigger all updaters' update action
     * @param conf the new worker conf
     */
    public void update(Map conf) {
        for (IUpdater updater : updaters) {
            try {
                updater.update(conf);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

}


