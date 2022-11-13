package com.stream.samples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author DeveoperZJQ
 * @since 2022/11/12
 */
public class MapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.112.147", 7777);

        SingleOutputStreamOperator<Integer> map = dataStream.map(String::length);

        map.print();

        env.execute(MapOperator.class.getSimpleName());
    }
}
