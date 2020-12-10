package com.stream.samples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2020-12-10
 * 测试聚合算子 max和reduce
 */
public class RollingAggregationMaxOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 25432);

//        SingleOutputStreamOperator<Tuple2<String, String>> maxRes = dataStream
//                .filter((FilterFunction<String>) String::isEmpty)
//                .map((MapFunction<String, Tuple2<String, String>>) s -> {
//                    String[] split = s.split(",");
//                    return Tuple2.of(split[0], split[1]);
//                })
//                .keyBy(0)
//                .maxBy(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = dataStream
                .filter((FilterFunction<String>) s -> !s.isEmpty())
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {

                        return Tuple2.of(s,1);
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                });

        dataStream
                .filter((FilterFunction<String>) s -> !s.isEmpty())
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] word = s.split(",");
                        String s1 = word[0];
                        Integer s2 = Integer.parseInt(word[1]);
                        return Tuple2.of(s1,s2);
                    }
                })
                .keyBy(0)
                .max(1).print();

        reduce.print();

        env.execute(RollingAggregationMaxOperator.class.getSimpleName());
    }
}
