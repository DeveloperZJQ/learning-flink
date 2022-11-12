package com.flink.datastream.join;

import com.alibaba.fastjson.JSON;
import com.flink.datastream.join.entity.ClickLog;
import com.flink.datastream.join.entity.OrderLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Join 的数据源
 *
 * @author DeveloperZJQ
 * @since 2022-11-10
 */
public final class JoinSource {
    private static final Logger log = LoggerFactory.getLogger(JoinSource.class);

    public static StreamExecutionEnvironment env() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // 获取一个数据源
    public static DataStream<ClickLog> socketSource(StreamExecutionEnvironment env) {
        DataStreamSource<String> clickSourceStream = env.socketTextStream("192.168.112.147", 7777);
        // 转换成DataStream<POJO>
        return clickSourceStream.map(message -> {
            log.info("receive ClickLog:{}", message);
            return JSON.parseObject(message, ClickLog.class);
        });
    }

    // 获取另一个数据源，当然也可以和上面的方法抽象成一个方法
    public static DataStream<OrderLog> socketAnotherSource(StreamExecutionEnvironment env) {
        DataStreamSource<String> orderSourceStream = env.socketTextStream("192.168.112.147", 7778);
        // 转换成DataStream<POJO>
        return orderSourceStream.map(message -> {
            log.info("receive OrderLog:{}", message);
            return JSON.parseObject(message, OrderLog.class);
        });
    }
}
