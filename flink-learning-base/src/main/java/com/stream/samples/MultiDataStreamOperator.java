package com.stream.samples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author happy
 * @since 2020-07-09 17:07
 * Union算子主要是将两个或者多个输入的数据集合集成一个数据集，需要保证两个数据集的格式一致，输出的数据集的格式和输入的数据集格式保持一致
 */
public class MultiDataStreamOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //union
        unionDemo(env);

        //connect
        connectMapDemo(env);
        connectFlatMapDemo(env);

        //split
        splitDemo(env);

        env.execute("MultiDataStreamOperator App start");
    }

    public static void unionDemo(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStream1 = env.fromElements("1", "2", "3");
        DataStreamSource<String> dataStream2 = env.fromElements("4", "5", "6");
        DataStreamSource<String> dataStream3 = env.fromElements("7", "8", "9");

        DataStream<String> union = dataStream1.union(dataStream2, dataStream3);
        union.print();
    }

    public static void connectMapDemo(StreamExecutionEnvironment env) {

        DataStreamSource<Tuple2<String, Integer>> dataTuples = env.fromElements(new Tuple2("hello", 1), new Tuple2("teacher", 2), new Tuple2("student", 3));
        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3);

        ConnectedStreams<Tuple2<String, Integer>, Integer> connect = dataTuples.connect(dataStream);

        SingleOutputStreamOperator<Tuple2<Integer, String>> coMapRes = connect.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map1(Tuple2 tuple2) throws Exception {
                String str = tuple2.f0.toString();
                Integer integer = Integer.valueOf(tuple2.f1.toString());
                return new Tuple2<>(integer, str);
            }

            @Override
            public Tuple2<Integer, String> map2(Integer integer) throws Exception {
                return new Tuple2<>(integer, "default");
            }
        });

        coMapRes.print();
    }

    public static void connectFlatMapDemo(StreamExecutionEnvironment env) {
        DataStreamSource<Tuple2<String, Integer>> dataTuples = env.fromElements(new Tuple2("hello", 1), new Tuple2("teacher", 2), new Tuple2("student", 3));
        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3);

        ConnectedStreams<Tuple2<String, Integer>, Integer> connect = dataTuples.connect(dataStream);

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> aDefault = connect.flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Integer, Tuple3<String, Integer, Integer>>() {
            @Override
            public void flatMap1(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                collector.collect(new Tuple3<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1, 0));
            }

            @Override
            public void flatMap2(Integer integer, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                collector.collect(new Tuple3<>("default", integer, 0));
            }
        });
    }

    public static void splitDemo(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStream = env.generateSequence(0, 9);
        SplitStream<Long> splitRes = dataStream.split((OutputSelector<Long>) aLong -> {
            ArrayList<String> output = new ArrayList<>();
            if (aLong % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");
            }
            return output;
        });

        DataStream<Long> even = splitRes.select("even");
        DataStream<Long> odd = splitRes.select("odd");
        DataStream<Long> all = splitRes.select("even", "odd");

        even.print();
        odd.print();
        all.print();
    }
}
