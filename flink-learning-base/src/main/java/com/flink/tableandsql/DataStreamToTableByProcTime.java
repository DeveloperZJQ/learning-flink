package com.flink.tableandsql;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author happy
 * @since 2022/6/12
 */
public class DataStreamToTableByProcTime {
    public static void main(String[] args) {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
// create a DataStream
        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

        Table table = tEnv.fromDataStream(dataStream, $("user_name"), $("user_action_time").proctime());
        table.window(
                Tumble.over(lit(10).minute())
                        .on($("user_action_time"))
                        .as("userActionWindow")
        );

    }
}
