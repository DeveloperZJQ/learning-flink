package com.stream.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author happy
 * @since 2020-12-17
 */
public class OfflineBroadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> broad = env.fromCollection(Arrays.asList(new Tuple2<>(1, "男"), new Tuple2<>(2, "女")));

        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9001);

        dataStreamSource.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                List<Tuple2<Integer,String>> broadName = getRuntimeContext().getBroadcastVariable("broadName");
                super.open(parameters);
            }

            @Override
            public String map(String s) throws Exception {
                return null;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        env.execute(OfflineBroadcast.class.getSimpleName());
    }
}
