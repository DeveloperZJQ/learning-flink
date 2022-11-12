package com.flink.datastream.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author DeveloperZJQ
 * @since 2022-6-5
 */
public class TumblingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = "192.168.112.147";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> orangeStream = env.socketTextStream(ip, 7777);
        DataStreamSource<String> greenStream = env.socketTextStream(ip, 7778);

        DataStream<String> joinStream = orangeStream.join(greenStream)
                .where(one -> one)
                .equalTo(two -> two)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .apply((JoinFunction<String, String, String>) (first, second) -> first + "," + second);

        joinStream.print();

        env.execute();
    }
}
