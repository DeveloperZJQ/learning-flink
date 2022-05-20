package com.project.shop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author DeveloperZJQ
 * @since 2022-5-20
 */
public class KeyedStatusDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            if (s != null && !s.equals("")) {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(Tuple2.of(s2, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        flatMap
                .keyBy(one -> one.f0)
                .flatMap(new Deduplicator())
                .print();
        env.execute(KeyedStatusDemo.class.getSimpleName());
    }


    public static class Deduplicator extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
            keyHasBeenSeen = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> event, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (keyHasBeenSeen.value() == null) {
                out.collect(event);
                keyHasBeenSeen.update(true);
            }
        }


    }

}
