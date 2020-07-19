package com.stream.streaming.event;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

/**
 * @author happy
 * @create 2020-07-10 14:14
 * 指定Timestamps与生成Watermarks有两种方式：
 *  1.在Source Function中直接定义Timestamp和Watermarks   具体实现见 EventTimeSourceFunctionDemo.java
 *  2.通过Flink自带的Timestamp Assigner指定Timestamp和生成Watermarks
 */
public class EventTimeAssignerDemo {
    /**
     *
     * @param args
     * 在使用了 flink 定义的外部数据源( 如 kafka) 之后, 就不能通过自定义 sourcefunction 的方式来生成 watermark 和 event time 了,
     * 这个时候可以使用 Timestamp Assigner, 其需要在第一个时间相关的 Operator前使用.
     * Flink 有自己定义好的 Timestamp Assigner 可以直接使用 (包括直接指定的方式和固定时间延迟的方式 ).
     * Flink 将 watermark 分为 Periodic Watermarks (根据设定的时间间隔周期性的生成) 和 Punctuated Watermarks (根据接入数量生成),
     * 用户也可以继承对应的类实现这两种 watermark.
     *
     */
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("input error,please confirm; eg args[0]=ip,args[1]=port");
            return;
        }
        StreamExecutionEnvironment env   = StreamExecutionEnvironment.getExecutionEnvironment();

        //系统默认的Watermark
        ascendingTimestampAssigner(env,args);

        //自己设置的固定的Watermark
//        boundedOutOfOrdernessTimestampAssigner(env,args);

        env.execute("EventTimeAssignerDemo");
    }

    /**
     * 使用Ascending Timestamp Assigner指定Timestamps 和 Watermarks
     * 通过调用DataStream API 中的assignAscendingTimestamps来指定Timestamp字段，不需要显示地指定Watermark，
     * 因为已经在系统中默认使用Timestamp创建Watermark
     * Flink 有自己定义好的 Timestamp Assigner 可以直接使用 (包括直接指定的方式和固定时间延迟的方式 )
     * @param env
     */
    public static void ascendingTimestampAssigner(StreamExecutionEnvironment env, String[] args){
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource       = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        SingleOutputStreamOperator<String> ascendRes    = dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                return Long.parseLong(s.split(",")[1]);
            }
        });
        //写到这里，我就非常好奇，那么源码底层Watermark是怎么安排的？
        ascendRes.print();
    }

    public static void boundedOutOfOrdernessTimestampAssigner(StreamExecutionEnvironment env,String[] args){
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[1]);
            }
        });

        stringSingleOutputStreamOperator.print();
    }
}
