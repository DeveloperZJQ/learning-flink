package com.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author DeveloperZJQ
 * @since 2022-5-30
 */
public class TupleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> wordCounts = env.fromElements(
                new Tuple2<>("hello", 1),
                new Tuple2<>("world", 2)
        );
        SingleOutputStreamOperator<Integer> map = wordCounts.map((MapFunction<Tuple2<String, Integer>, Integer>) value -> value.f1);
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordCounts.keyBy(one -> one.f0);
        map.print();
        keyBy.print();

        env.execute();
    }
}