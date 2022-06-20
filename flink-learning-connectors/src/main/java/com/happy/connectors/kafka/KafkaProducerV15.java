package com.happy.connectors.kafka;

import com.happy.connectors.kafka.event.KafkaEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * flin1.15 版本kafka连接器
 *
 * @author happy
 * @since 2022/6/19
 */
public class KafkaProducerV15 {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("请传入有效信息，如192.168.2.112:9092");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.112.147", 9999, "\n", 3);

        SingleOutputStreamOperator<KafkaEvent> map = source.filter(Objects::nonNull).map((MapFunction<String, KafkaEvent>) value -> {
            String[] words = value.split(" ");
            if (words.length != 2) {
                return null;
            }
            Integer id = Integer.parseInt(words[0]);
            String name = words[1];
            return new KafkaEvent(id, name);
        }).uid("111111");

        SingleOutputStreamOperator<String> process = map.process(new ProcessFunction<>() {
            @Override
            public void processElement(KafkaEvent kafkaEvent, Context context, Collector<String> collector) {
                collector.collect(kafkaEvent.getName());
            }
        });

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(args[0])
                .setRecordSerializer(KafkaRecordSerializationSchema
                        .builder()
                        .setTopic("test002")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        process.print("process-->");
        process.sinkTo(kafkaSink);

        env.execute("Flink DataSource");
    }
}