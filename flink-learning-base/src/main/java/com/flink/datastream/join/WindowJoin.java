package com.flink.datastream.join;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author DeveloperZJQ
 * @since 2022-11-7
 * join() 的语义即 Window join
 */
public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> clickSourceStream = env.socketTextStream("127.0.0.1", 7777);
        DataStreamSource<String> orderSourceStream = env.socketTextStream("127.0.0.1", 7778);
        // 转换成DataStream<POJO>
        DataStream<ClickLog> clickLog = clickSourceStream.map(message -> JSON.parseObject(message, ClickLog.class));
        DataStream<OrderLog> orderLog = orderSourceStream.map(message -> JSON.parseObject(message, OrderLog.class));

        DataStreamSink<String> joinStream = clickLog
                .join(orderLog)
                .where(record -> record.goodId)
                .equalTo(record -> record.goodId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(
                        (JoinFunction<ClickLog, OrderLog, String>)
                                (accessRecord, orderRecord) -> StringUtils.join(Arrays.asList(
                                        accessRecord.goodId,
                                        orderRecord.getGoodName()
                                ), '\t'))
                .print().setParallelism(1);

        DataStreamSink<Tuple2<String, String>> coGroupStream = clickLog
                .coGroup(orderLog)
                .where(ClickLog::getGoodId)
                .equalTo(OrderLog::getGoodId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
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
                })
                .print().setParallelism(1);


        DataStreamSink<String> intervalStream = clickLog
                .keyBy(ClickLog::getGoodId)
                .intervalJoin(orderLog.keyBy(record -> record.goodId))
                .between(Time.seconds(-30), Time.seconds(30))
                .process(new ProcessJoinFunction<ClickLog, OrderLog, String>() {
                    @Override
                    public void processElement(ClickLog accessRecord, OrderLog orderRecord, Context context, Collector<String> collector) throws Exception {
                        collector.collect(StringUtils.join(Arrays.asList(
                                accessRecord.getGoodId(),
                                orderRecord.getGoodName()
                        ), '\t'));
                    }
                })
                .print().setParallelism(1);

        env.execute(WindowJoin.class.getSimpleName());
    }

    @Data
    public static class ClickLog {
        private String sessionId;
        private String goodId;
    }

    @Data
    public static class OrderLog {
        private String goodId;
        private String goodName;
    }
}
