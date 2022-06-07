package com.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 本part是flink实验的一个案例，暂时不能执行成功
 * <Reinterpreting a pre-partitioned data stream as keyed stream>
 *
 * @author DeveloperZJQ
 * @since 2022-6-7
 */
public class ExperimentalFeatures {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = "127.0.0.1";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> source = env.socketTextStream(ip, 9999);

        SingleOutputStreamOperator<Integer> map = source.map((MapFunction<String, Integer>) Integer::parseInt);

        DataStreamUtils.reinterpretAsKeyedStream(map, (in) -> in, TypeInformation.of(Integer.class))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(Integer::sum)
                .addSink(new DiscardingSink<>());

        env.execute();
    }
}
