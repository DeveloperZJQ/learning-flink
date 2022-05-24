package com.flink.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2022/5/23
 */
public class Iterations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enc = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> someIntegers = enc.generateSequence(0, 1000);
        IterativeStream<Long> iterate = someIntegers.iterate();
        SingleOutputStreamOperator<Long> minusOne = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                return aLong - 1;
            }
        });

        SingleOutputStreamOperator<Long> filter = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong > 0;
            }
        });

        iterate.closeWith(filter);
        SingleOutputStreamOperator<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong <= 0;
            }
        });

        lessThanZero.print();
        DataStreamUtils.collect(filter);
        enc.execute();
    }
}
