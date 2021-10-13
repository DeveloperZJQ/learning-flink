package com.happy.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author happy
 * @since 2020-12-15
 * 动态加载topics
 */
public class DynamicLoadTopicConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Pattern topics = Pattern.compile("test-topic-[0-9]");
        Properties props = new Properties();
        props.load(DynamicLoadTopicConsumer.class.getResourceAsStream("kafka/consumer.properties"));
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>(topics, new SimpleStringSchema(), props));

        dataStreamSource.print();
        env.execute(DynamicLoadTopicConsumer.class.getSimpleName());
    }
}
