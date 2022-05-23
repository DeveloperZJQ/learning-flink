package com.stream.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2020-07-04
 * word count
 */
public class WCDemo {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.fromElements(WORDS)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] s1 = s.split("\\W+");

                    for (String word : s1) {
                        if (word.length() > 0) {
                            collector.collect(new Tuple2<>(word.trim(), 1));
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(one -> one.f0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (stringIntegerTuple2, t1) -> new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + stringIntegerTuple2.f1))
                .print();

        env.execute("start ...");
    }

    private static final String[] WORDS = new String[]{
            "To be , or not to be , -- that is the question : --",
            "whether 'tis nobler in the mind to suffer'"
    };

}