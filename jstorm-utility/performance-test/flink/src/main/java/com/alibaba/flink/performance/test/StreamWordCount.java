package com.alibaba.flink.performance.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.addS
        ParameterTool tool = ParameterTool.fromArgs(args);
        int sourceParallel = Integer.parseInt(tool.get("s"));
        int operatorParallel = Integer.parseInt(tool.get("p"));

        System.out.println("sourceParallel: " + sourceParallel + ", operatorParallel: " + operatorParallel);

        env.setParallelism(operatorParallel);

        // get input data
        DataStream<String> text = env.addSource(new WordSource()).setParallelism(sourceParallel);

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum(1);

        // execute program
//        env.execute("StreamWordCount");
        System.out.println(env.getExecutionPlan());
    }

    public static class WordSource implements ParallelSourceFunction<String> {

        Random _rand = ThreadLocalRandom.current();
        private volatile boolean active = true;
        private static final String[] CHOICES = {
                "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers"
        };

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (active) {
                String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
                ctx.collect(sentence);
            }
        }

        @Override
        public void cancel() {
            active = false;
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
