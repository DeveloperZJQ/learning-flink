package com.stream.samples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author DeveloperZJQ
 * @since 2022/11/13
 */
public class CustomFlatMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.112.147", 7777);

        SingleOutputStreamOperator<String> flatMap = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split("");
                StringBuilder ori = new StringBuilder();
                for (String s : split) {
                    ori.insert(0, s);
                }
                // 过滤
                if (ori.toString().equals("olleh")) {
                    return;
                }
                out.collect(ori.toString());
            }
        });

        flatMap.print();

        env.execute(MapOperator.class.getSimpleName());
    }
}
