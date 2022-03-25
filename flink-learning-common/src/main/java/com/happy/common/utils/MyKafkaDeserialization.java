package com.happy.common.utils;

import com.alibaba.fastjson.JSON;
import com.happy.common.model.MetricEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

public class MyKafkaDeserialization implements KafkaDeserializationSchema<MetricEvent> {
    private static final Logger log = Logger.getLogger(MyKafkaDeserialization.class);
    private final String encoding = "UTF8";
    private boolean includeTopic;
    private boolean includeTimestamp;

    public MyKafkaDeserialization(boolean includeTopic, boolean includeTimestamp) {
        this.includeTopic = includeTopic;
        this.includeTimestamp = includeTimestamp;
    }

    public MyKafkaDeserialization() {
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent nextElement) {
        return false;
    }

    @Override
    public MetricEvent deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (consumerRecord != null) {
            try {
                String value = new String(consumerRecord.value(), encoding);
                //                if (includeTopic) jason.setTopic(consumerRecord.topic());
//                if (includeTimestamp) jason.setTimestamp(consumerRecord.timestamp());
                return JSON.parseObject(value, MetricEvent.class);
            } catch (Exception e) {
                log.error("deserialize failed : " + e.getMessage());
            }
        }
        return null;
    }
}