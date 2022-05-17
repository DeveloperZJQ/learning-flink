package com.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author happy
 * @since 2022/5/17
 */
public class ClickPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> clicks = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicks.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String userID = s.split(",")[0];
                return Tuple2.of(userID, 1L);
            }
        })
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.minutes(30L)))
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));

        result.print();
        env.execute();
    }
}
