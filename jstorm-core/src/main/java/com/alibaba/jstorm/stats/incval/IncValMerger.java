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
package com.alibaba.jstorm.stats.incval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class IncValMerger extends RunnableCallback {
	/**
	 * Merget List<Map<Object, Long>> to Map<Object, Long>
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Object execute(T... args) {
		Map<Object, Long> result = null;
		if (args != null && args.length > 0) {

			List<Map<Object, Long>> list = (List<Map<Object, Long>>) args[0];
			result = new HashMap<Object, Long>();

			for (Map<Object, Long> each : list) {

				for (Entry<Object, Long> e : each.entrySet()) {
					Object key = e.getKey();
					Long val = e.getValue();
					if (result.containsKey(key)) {
						val = (Long) JStormUtils.add(val, result.get(key));
					}
					result.put(key, val);
				}
			}
		}
		return result;
	}
}
