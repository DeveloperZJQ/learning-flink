package com.flink.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author happy
 * @since 2022/5/23
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> withTimestampsAndWatermarks = stream.filter(event -> event.length() > 0)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofMinutes(1)));

        WatermarkStrategy<Tuple2<Long, String>> watermarkStrategy = WatermarkStrategy.<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(2));

        withTimestampsAndWatermarks.keyBy((event) -> event)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<String>) (s, t1) -> {
                    long l = Long.parseLong(s) + Long.parseLong(t1);
                    return String.valueOf(l);
                }).print();

        env.execute();
    }
}
