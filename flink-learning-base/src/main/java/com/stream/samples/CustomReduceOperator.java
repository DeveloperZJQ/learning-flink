package com.stream.samples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author DeveloperZJQ
 * @since 2022/11/13
 */
public class CustomReduceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.112.147", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(
                (MapFunction<String, Tuple2<String, Integer>>)
                        value -> Tuple2.of(value, value.length())).returns(Types.TUPLE(Types.STRING, Types.INT));


        // map-> keyBy-> reduce-> print
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = map.keyBy(value -> value.f0)
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1));

        // map-> keyBy-> window-> reduce-> print
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceWindow = map.keyBy((in) -> in, Types.TUPLE(Types.STRING, Types.INT))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1));


        reduce.print("reduce->");
        reduceWindow.print("reduceWindow->");

        env.execute(CustomReduceOperator.class.getSimpleName());
    }
}
