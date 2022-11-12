package com.flink.datastream.join;

import com.flink.datastream.join.entity.ClickLog;
import com.flink.datastream.join.entity.OrderLog;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * coGroup
 *
 * @author DeveloperZJQ
 * @since 2022-11-10
 */
public class CoGroupsJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JoinSource.env();
        DataStream<ClickLog> clickLogDataStream = JoinSource.socketSource(env);
        DataStream<OrderLog> orderLogDataStream = JoinSource.socketAnotherSource(env);

        DataStream<Tuple2<String, String>> coGroupStream = clickLogDataStream
                .coGroup(orderLogDataStream)
                .where(ClickLog::getGoodId)
                .equalTo(OrderLog::getGoodId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply((CoGroupFunction<ClickLog, OrderLog, Tuple2<String, String>>) (accessRecords, orderRecords, collector) -> {
                    for (ClickLog accessRecord : accessRecords) {
                        boolean isMatched = false;
                        for (OrderLog orderRecord : orderRecords) {
                            // 右流中有对应的记录
                            collector.collect(new Tuple2<>(accessRecord.getGoodId(), orderRecord.getGoodName()));
                            isMatched = true;
                        }
                        if (!isMatched) {
                            // 右流中没有对应的记录
                            collector.collect(new Tuple2<>(accessRecord.getGoodId(), null));
                        }
                    }
                });

        coGroupStream.print().setParallelism(1);

        env.execute(CoGroupsJoin.class.getSimpleName());
    }
}
