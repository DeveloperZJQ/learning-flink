package com.happy.connectors.kafka;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author happy
 * @since  2022/07/09
 */
@Deprecated(since = "before flink1.15")
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("请传入有效信息，如192.168.2.112:9092");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream(args[0].split(":")[0], 9995);

        //Properties参数定义
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//key反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value反序列化
/*
        FlinkKafkaProducer011<String> metric = new FlinkKafkaProducer011<>(
                "metric",
                new SimpleStringSchema(),
                props
        );

        dataStream.addSink(metric);
*/
        env.execute("KafkaProducer App addSink");
    }
}