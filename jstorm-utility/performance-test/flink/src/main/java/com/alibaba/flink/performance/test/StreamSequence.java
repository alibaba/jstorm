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
package com.alibaba.flink.performance.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class StreamSequence {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<Long> text = env.addSource(new SequenceSource(10)).slotSharingGroup("123");

        DataStream<Long> counts = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                return aLong;
            }
        });

//        counts.print();

//        counts.print();

        // execute program
        env.execute("StreamSequence");
    }

    public static class SequenceSource implements SourceFunction<Long> {
        private volatile boolean active = true;
        private int sendNumEachTime;
        private long count = 0;

        public SequenceSource(int sendNumEachTime) {
            this.sendNumEachTime = sendNumEachTime;
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while(active) {
                int sendNum = sendNumEachTime;
                while (--sendNum >= 0) {
                    ++count;
                    ctx.collect(count);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
