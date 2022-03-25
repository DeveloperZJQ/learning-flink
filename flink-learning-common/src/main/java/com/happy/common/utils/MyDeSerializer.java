package com.happy.common.utils;

import com.happy.common.model.MetricEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author DeveloperZJQ
 * @since 2022-3-25
 */
public class MyDeSerializer implements DeserializationSchema<MetricEvent> {
    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return null;
    }
}
