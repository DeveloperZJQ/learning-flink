package com.flink.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @since 2022/5/23
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        String ip = "localhost";
        if (args.length > 0) {
            ip = args[0];
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream(ip, 9999)
                .flatMap(new Splitter())
                .keyBy(one -> one.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] sentence = s.split(" ");
            for (String word : sentence) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
