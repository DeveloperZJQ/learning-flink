package com.stream.samples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author happy
 * @create 2020-07-09 12:28
 * Map [DataStream->DataStream]
 * FlatMap [DataStream->DataStream]
 * filter [DataStream->DataStream]
 * keyBy [DataStream->DataStream]
 * reduce [KeyedStream->DataStream]
 * aggregations [KeyedStream->DataStream]
 */
public class SingleDataStreamOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //map
        mapDemo(env);

        //flatMap
        flatMapDemo(env);

        //filter
        filterDemo(env);

        //keyBy
        keyByDemo(env);

        //reduce
        reduceDemo(env);

        //aggregations
        //该算子是DataStream接口提供得，根据指定得字段进行聚合操作，滚动地产生一系列数据聚合结果。其实是将Reduce算子中的函数进行了封装，
        //封装的聚合操作有sum，min，minBy，max，maxBy等，这样就不需要用户自己定义Reduce函数。
        keyByDemo(env);

        env.execute("TransformOperator App start");
    }

    public static void mapDemo(StreamExecutionEnvironment env){
        //数据集
        DataStreamSource<Integer> dataStream    = env.fromElements(1, 2, 3, 4);
        //指定map计算表达式
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        });
        map.print();
    }

    public static void flatMapDemo(StreamExecutionEnvironment env){
        String[] arr                                    = {"string", "flink", "spark", "hadoop", "hive", "airflow", "greenplum"};
        DataStreamSource<String> dataStream             = env.fromCollection(Arrays.asList(arr));
        SingleOutputStreamOperator<Integer> flatMapRes  = dataStream.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String s, Collector<Integer> collector) throws Exception {
                collector.collect(s.length());
            }
        });

        flatMapRes.print();
    }

    public static void filterDemo(StreamExecutionEnvironment env){
        String[] arr   =   {"1","2","3","4","5","6"};
        DataStreamSource<String> dataStream             = env.fromCollection(Arrays.asList(arr));
        SingleOutputStreamOperator<Integer> filter = dataStream.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String s, Collector<Integer> collector) throws Exception {
                collector.collect(Integer.parseInt(s));
            }
        })
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer integer) throws Exception {
                        return integer > 3;
                    }
                });
        filter.print();
    }

    public static void keyByDemo(StreamExecutionEnvironment env){
        DataStreamSource<String> dataStream = env.fromElements("hello", "word", "earth", "google", "twins");
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByRes = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(s, 1));
            }
        })
                .keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum         = keyByRes.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> min         = keyByRes.min(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> max         = keyByRes.max(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> minByRes    = keyByRes.minBy(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxByRes    = keyByRes.maxBy(1);

        sum.print();
    }

    public static void reduceDemo(StreamExecutionEnvironment env){
        String[] WORDSTEST = new String[]{
                "To be , or not to be , -- that is the question : --",
                "whether 'tis nobler in the mind to suffer'"
        };

        DataStreamSource<String> dataStream = env.fromElements(WORDSTEST);
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split("\\W+");

                        for (String word : s1) {
                            if (word.length() > 0) {
                                collector.collect(new Tuple2<>(word.trim(), 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + stringIntegerTuple2.f1);
                    }
                });

        reduce.print();
    }

}
