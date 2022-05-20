package com.basic;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将相邻的keyed START 和END 事件相匹配并计算两者的时间间隔
 * 输入数据为Tuple2<String，String>类型，第一个字段为key值，第二个字段标记为START 和END 事件
 *
 * @author happy
 * @since 2022/5/16
 */
public class StartEndDuration extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {
    private ValueState<Long> startTime;

    @Override
    public void open(Configuration parameters) {
        startTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("startTime", Long.class));
    }

    @Override
    public void processElement(Tuple2<String, String> in, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
        switch (in.f1) {
            case "START": {
                startTime.update(context.timestamp());
                context.timerService().registerEventTimeTimer(context.timestamp() + 4 * 60 * 60 * 1000);
                break;
            }
            case "END": {
                Long sTime = startTime.value();
                if (sTime != null) {
                    collector.collect(Tuple2.of(in.f0, context.timestamp() - sTime));
                    startTime.clear();
                }
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        startTime.clear();
    }
}
