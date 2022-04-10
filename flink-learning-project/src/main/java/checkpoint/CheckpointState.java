package checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author happy
 * @since 2022/4/6
 */
public class CheckpointState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 9000)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] split = s.split(",");
                    for (String s1 : split) {
                        Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>();
                        stringIntegerTuple2.f0 = s1;
                        stringIntegerTuple2.f1 = 1;
                        collector.collect(stringIntegerTuple2);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.getConfig().disableGenericTypes();

        env.fromElements(Arrays.asList("hello", "world", "men"))
                .flatMap((FlatMapFunction<List<String>, Tuple2<String, Integer>>) (list, collector) -> {
                    for (String s : list) {
                        collector.collect(Tuple2.of(s, 1));
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.execute();
    }
}
