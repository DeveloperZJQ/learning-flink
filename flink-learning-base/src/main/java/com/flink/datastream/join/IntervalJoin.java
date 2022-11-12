package com.flink.datastream.join;

import com.flink.datastream.join.entity.ClickLog;
import com.flink.datastream.join.entity.OrderLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;

/**
 * interval join
 *
 * @author DeveloperZJQ
 * @since 2022-11-10
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JoinSource.env();
        DataStream<ClickLog> clickLogDataStream = JoinSource.socketSource(env);
        DataStream<OrderLog> orderLogDataStream = JoinSource.socketAnotherSource(env);

        SingleOutputStreamOperator<ClickLog> clickLogWatermark = clickLogDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<ClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimeStamp()));

        SingleOutputStreamOperator<OrderLog> orderLogWatermark = orderLogDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderLog>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimeStamp()));

        SingleOutputStreamOperator<String> intervalStream = clickLogWatermark
                .keyBy(ClickLog::getGoodId)
                .intervalJoin(orderLogWatermark.keyBy(OrderLog::getGoodId))
                .between(Time.seconds(-30), Time.seconds(30))
                .process(new ProcessJoinFunction<>() {
                    @Override
                    public void processElement(ClickLog accessRecord, OrderLog orderRecord, Context context, Collector<String> collector) throws Exception {
                        collector.collect(StringUtils.join(Arrays.asList(
                                accessRecord.getGoodId(),
                                orderRecord.getGoodName()
                        ), '\t'));
                    }
                });
        intervalStream.print().setParallelism(1);

        env.execute(IntervalJoin.class.getSimpleName());
    }
}
