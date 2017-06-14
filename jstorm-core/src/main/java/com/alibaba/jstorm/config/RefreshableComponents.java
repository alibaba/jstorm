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
