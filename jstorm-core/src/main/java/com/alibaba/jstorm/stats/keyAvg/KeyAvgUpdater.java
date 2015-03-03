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
package com.alibaba.jstorm.stats.keyAvg;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.stats.StatFunction;
import com.alibaba.jstorm.stats.rolling.UpdateParams;
import com.alibaba.jstorm.utils.Pair;

public class KeyAvgUpdater extends RunnableCallback {

	@SuppressWarnings("unchecked")
	@Override
	public <T> Object execute(T... args) {
		Map<Object, Pair<Long, Long>> curr = null;
		if (args != null && args.length > 0) {
			UpdateParams p = (UpdateParams) args[0];
			if (p.getCurr() != null) {
				curr = (Map<Object, Pair<Long, Long>>) p.getCurr();
			} else {
				curr = new HashMap<Object, Pair<Long, Long>>();
			}
			Object[] keyAvgArgs = p.getArgs();

			Long amt = 1l;
			if (keyAvgArgs.length > 1) {
				amt = Long.parseLong(String.valueOf(keyAvgArgs[1]));
			}
			StatFunction.update_keyed_avg(curr, keyAvgArgs[0], amt);
		}
		return curr;
	}
}
