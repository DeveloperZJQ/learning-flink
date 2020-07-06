package com.stream.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @create 2020-07-04
 * word count
 */
public class WCDemo {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split("\\W+");

                        for (String word :s1){
                            if (word.length()>0){
                                collector.collect(new Tuple2<>(word.trim(),1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1+stringIntegerTuple2.f1);
                    }
                })
                .print();

        env.execute("start ...");
    }

    private static final String[] WORDS = new String[]{
        "To be , or not to be , -- that is the question : --",
        "whether 'tis nobler in the mind to suffer'"
    };
}

