package com.flink.datastream.join;

import com.flink.datastream.join.entity.ClickLog;
import com.flink.datastream.join.entity.OrderLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * @author DeveloperZJQ
 * @since 2022-11-7
 * join() 的语义即 Window join
 */
public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JoinSource.env();
        DataStream<ClickLog> clickLogDataStream = JoinSource.socketSource(env);
        clickLogDataStream.print("clickLog:");
        DataStream<OrderLog> orderLogDataStream = JoinSource.socketAnotherSource(env);
        orderLogDataStream.print("orderLog:");

        DataStream<String> joinStream = clickLogDataStream
                .join(orderLogDataStream)
                .where(ClickLog::getGoodId)
                .equalTo(OrderLog::getGoodId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(
                        (JoinFunction<ClickLog, OrderLog, String>)
                                (accessRecord, orderRecord) -> StringUtils.join(Arrays.asList(
                                        accessRecord.getGoodId(),
                                        orderRecord.getGoodName()
                                ), '\t'));

        joinStream.print().setParallelism(1);

        env.execute(WindowJoin.class.getSimpleName());
    }
}
