package com.flink.datastream.join;

import com.flink.datastream.join.entity.ClickLog;
import com.flink.datastream.join.entity.OrderLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect
 *
 * @author DeveloperZJQ
 * @since 2022/11/12
 */
public class ConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = JoinSource.env();
        DataStream<ClickLog> clickLogDataStream = JoinSource.socketSource(env);
        clickLogDataStream.print("clickLog:");
        DataStream<OrderLog> orderLogDataStream = JoinSource.socketAnotherSource(env);
        orderLogDataStream.print("orderLog:");

        SingleOutputStreamOperator<Tuple2<String, String>> connectStream = clickLogDataStream.connect(orderLogDataStream).map(new CoMapFunction<ClickLog, OrderLog, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map1(ClickLog clickLog) {
                return Tuple2.of(clickLog.getGoodId(), clickLog.getSessionId());
            }

            @Override
            public Tuple2<String, String> map2(OrderLog orderLog) {
                return Tuple2.of(orderLog.getGoodId(), orderLog.getGoodName());
            }
        });

        connectStream.print().setParallelism(1);

        env.execute(WindowJoin.class.getSimpleName());
    }
}
