package com.happy.connectors.elasticsearch;

import com.happy.connectors.kafka.KafkaConsumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author happy
 * @since 2020-11-05 22:44
 */
public class ElasticSearchMain {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("请传入有效信息，如192.168.2.112:9092");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Properties参数定义
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//key反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value反序列化

        props.put("auto.offset.reset", "latest");//偏移量最新earliest

        KafkaSource<String> consumer = KafkaSource.<String>builder()
                .setTopics("metric")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(props)
                .build();
        DataStreamSource<String> metricStreamSource = env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(1);


        SingleOutputStreamOperator<String> myCounter = metricStreamSource.process(new ProcessFunction<>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) {
                long l = System.currentTimeMillis();
                logger.info("current timestamp is :" + l);
//                getRuntimeContext().getMetricGroup();

//                getRuntimeContext().getMetricGroup().counter(3).getCount();
            }
        });


        myCounter.print();

        metricStreamSource.print(); //把从kafka读取到的数据打印并输出

        env.execute("Flink DataSource");
    }
}