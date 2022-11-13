package com.stream.samples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author DeveloperZJQ
 * @since 2022/11/13
 */
public class CustomFilterAndKeyByOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.112.147", 7777);

        SingleOutputStreamOperator<String> filter = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                // 保留字符串长度大于5的
                return value.length() > 5;
            }
        });

        KeyedStream<String, String> keyBy = filter.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) {
                return value;
            }
        });

        filter.print();
        keyBy.print();

        env.execute(MapOperator.class.getSimpleName());
    }
}
