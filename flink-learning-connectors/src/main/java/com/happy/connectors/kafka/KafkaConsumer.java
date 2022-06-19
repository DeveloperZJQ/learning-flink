package com.happy.connectors.kafka;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author happy
 * @since 2020-07-08 22:21
 */
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("请传入有效信息，如192.168.2.112:9092");
            return;
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers(args[0])
                .setProperties(props)
                .setTopics(Arrays.asList("test002".split(" ")))
                .setGroupId("metric001")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> metricStreamSource = env.fromSource(build, WatermarkStrategy.noWatermarks(), "Kafka Source");

        metricStreamSource.print();
        SingleOutputStreamOperator<String> myCounter = metricStreamSource.process(new ProcessFunction<>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) {
                long l = System.currentTimeMillis();
                logger.info("current timestamp is :" + l);
                getRuntimeContext().getMetricGroup().counter(3).getCount();
            }
        });

        myCounter.print();

        env.execute("Flink DataSource");
    }
}