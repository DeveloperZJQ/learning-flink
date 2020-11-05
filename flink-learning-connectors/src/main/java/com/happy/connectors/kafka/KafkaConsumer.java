package com.happy.connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author happy
 * @create 2020-07-08 22:21
 *
 */
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    public static void main(String[] args) throws Exception {
        if (args.length!=1){
            System.err.println("请传入有效信息，如192.168.2.112:9092");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Properties参数定义
        Properties props = new Properties();
        props.put("bootstrap.servers",args[0]);
        props.put("group.id","metric-group");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");//key反序列化
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");//value反序列化

        props.put("auto.offset.reset","latest");//偏移量最新earliest

        DataStreamSource<String> metricStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "metric",   //kafka topic
                new SimpleStringSchema(),//String 序列化
                props
        )).setParallelism(1);

        SingleOutputStreamOperator<String> myCounter = metricStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                long l = System.currentTimeMillis();
                logger.info("current timestamp is :"+l);
                getRuntimeContext().getMetricGroup().counter(3).getCount();
            }
        });

        myCounter.print();

        metricStreamSource.print(); //把从kafka读取到的数据打印并输出

        env.execute("Flink DataSource");
    }
}