package com.happy.metrics.counter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * @author happy
 * @since 2020-11-12
 */
public class CounterMyMapper extends RichMapFunction<String, String> {
    private transient Counter counter;

    @Override
    public void open(Configuration config) {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
    }

    @Override
    public String map(String value) throws Exception {
        this.counter.inc();
        return value;
    }
}
