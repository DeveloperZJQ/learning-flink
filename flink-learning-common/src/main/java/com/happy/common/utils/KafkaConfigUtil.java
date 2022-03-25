package com.happy.common.utils;

import com.happy.common.constant.PropertiesConstants;
import com.happy.common.model.MetricEvent;
import com.happy.common.schema.MetricSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.happy.common.constant.PropertiesConstants.*;

/**
 * @author happy
 * @create 2020-09-04 18:31
 */
public class KafkaConfigUtil {

    /**
     * 设置基础的 Kafka 配置
     *
     * @return
     */
    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    /**
     * 设置 kafka 配置
     *
     * @param parameterTool
     * @return
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        props.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }


    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) throws IllegalAccessException {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameter.getRequired(PropertiesConstants.METRICS_TOPIC);
        Long time = parameter.getLong(PropertiesConstants.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time);
    }

    public static DataStreamSource<String> buildSourceString(StreamExecutionEnvironment env) throws IllegalAccessException {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String requiredTopic = parameter.getRequired(METRICS_TOPIC);
        Long time = parameter.getLong(CONSUMER_FROM_TIME, 0L);
        return buildSourceString(env, requiredTopic, time);
    }

    public static DataStreamSource<String> buildSourceString(StreamExecutionEnvironment env, String requiredTopic, Long time) {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        KafkaSource<String> consumer = KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(requiredTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "KafkaSource");
    }

    /**
     * @param env
     * @param topic
     * @param time  订阅的时间
     */
    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env, String topic, Long time) throws IllegalAccessException {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        KafkaSource<MetricEvent> consumer = KafkaSource.<MetricEvent>builder()
                .setTopics(topic)
                .setValueOnlyDeserializer(new MyDeSerializer())
                .setProperties(props)
                .setStartingOffsets(OffsetsInitializer.timestamp(time))
                .build();
        return env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }


    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
        props.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(PropertiesConstants.METRICS_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));

        consumer.close();
        return partitionOffset;
    }
}