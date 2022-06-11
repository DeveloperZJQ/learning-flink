package com.flink.tableandsql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author happy
 * @since 2022/6/11
 */
public class TableStreamExplain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStreamSource<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

//        Schema build = Schema.newBuilder()
//                .column("count", DataTypes.INT())
//                .column("word", DataTypes.STRING())
//                .build();
        Table table1 = tEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tEnv.fromDataStream(stream2, $("count"), $("word"));

        Table table = table1.where($("word").like("F%")).unionAll(table2);

        System.out.println(table.explain());
    }
}
