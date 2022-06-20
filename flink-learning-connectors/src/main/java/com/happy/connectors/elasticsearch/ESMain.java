package com.happy.connectors.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author happy
 * @since 2020-11-12
 */
@Slf4j
public class ESMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Properties参数定义
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//key反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value反序列化

        props.put("auto.offset.reset", "latest");//偏移量最新earliest
/*
        DataStreamSource<String> metricStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "test",   //kafka topic
                new SimpleStringSchema(),//String 序列化
                props
        )).setParallelism(1);

        //批量写入elasticsearch
        metricStreamSource.addSink(new ESHighRestBulkProcessorWriter()).name("SinkES");

 */
        env.execute("Flink DataSource");
    }
}