package flink.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

/**
 * @author happy
 * @since 2022/4/7
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        String ip = args[0];
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream(ip, 3306);

        final int windowSize = 10;
        final int slideSize = 5;

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator =
                streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    if (!StringUtils.isNullOrWhitespaceOnly(s)) {
                        for (String s1 : s.split(",")) {
                            Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>();
                            stringIntegerTuple2.f0 = s1;
                            stringIntegerTuple2.f1 = 1;
                            collector.collect(stringIntegerTuple2);
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                        .setParallelism(1)
//                        .slotSharingGroup("flatMap_sg")
//                        .keyBy(0)
                        .keyBy(
                                (KeySelector<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                                        value -> Tuple2.of(value.f0, value.f1),
                                Types.TUPLE(Types.STRING, Types.INT)
                        )
                        .countWindow(windowSize, slideSize)
                        .sum(1)
                        .setParallelism(1);
//                        .slotSharingGroup("sum_sg");

//        streamOperator.print().setParallelism(1);
        streamOperator.print();

        env.execute(WindowWordCount.class.getSimpleName());
    }
}
