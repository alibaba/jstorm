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
