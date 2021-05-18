package ch3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @since 2021-02-02
 */
public class ThirdPointThree {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.readTextFile("C:\\Users\\18380\\myself\\coder\\learning-flink\\flink-learning-book\\README.md", "utf-8");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = text.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            String[] split = s.split(" ");
            for (String s1 : split) {
                collector.collect(s1);
            }
        }).filter((FilterFunction<String>) s -> !s.isEmpty()).map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s, 1)).keyBy(0).sum(1);

        sum.print();

        env.execute(ThirdPointThree.class.getSimpleName());
    }
}
