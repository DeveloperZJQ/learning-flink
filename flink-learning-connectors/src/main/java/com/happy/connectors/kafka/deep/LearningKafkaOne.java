package com.happy.connectors.kafka.deep;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author happy
 * @since 2021-12-22
 */
public class LearningKafkaOne {
    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }


}
