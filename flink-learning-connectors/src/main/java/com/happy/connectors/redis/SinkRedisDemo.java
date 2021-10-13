package com.happy.connectors.redis;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2020-12-11
 */
public class SinkRedisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9001);

        dataStreamSource.print();
        env.execute(SinkRedisDemo.class.getSimpleName());
    }
}
