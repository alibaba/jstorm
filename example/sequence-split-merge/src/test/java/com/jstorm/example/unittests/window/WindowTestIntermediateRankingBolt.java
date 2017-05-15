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
package com.jstorm.example.unittests.window;

import backtype.storm.tuple.Tuple;
import org.apache.storm.starter.tools.Rankable;
import org.apache.storm.starter.tools.RankableObjectWithFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author binyang.dby on 2016/7/11.
 */
public class WindowTestIntermediateRankingBolt extends WindowTestAbstractRankingBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowTestIntermediateRankingBolt.class);

    public WindowTestIntermediateRankingBolt(int topN) {
        super(topN);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
}
