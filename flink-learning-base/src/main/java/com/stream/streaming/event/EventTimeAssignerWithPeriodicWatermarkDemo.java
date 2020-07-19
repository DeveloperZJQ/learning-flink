package com.stream.streaming.event;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author happy
 * @create 2020-07-10 15:26
 */
public class EventTimeAssignerWithPeriodicWatermarkDemo {
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("");
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream(args[0], 9995);
        //自定义Timestamp Assigner和Watermark Generator
        //1.Periodic Watermarks自定义生成
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(new PeriodicAssigner());
        stringSingleOutputStreamOperator.print();

        //2.Punctuated Watermarks自定义生成
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator1 = dataStreamSource.assignTimestampsAndWatermarks(new PunctuatedAssigner());
        stringSingleOutputStreamOperator1.print();


        env.execute("EventTimeAssignerWithPeriodicWatermarkDemo App start");
    }
}

/**
 * 通过实现AssignerWithPeriodicWatermarks接口自定义生成Watermark
 */
class PeriodicAssigner implements AssignerWithPeriodicWatermarks<String> {
    Long maxOutOfOrderness      =   1000L;      //1s延时设定，表示在1秒内的数据延时有效，超过一秒的数据被认定为迟到事件
    Long currentMaxTimestamp    ;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {//复写getCurrentWatermark方法，生成水位线时间Watermark
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(String s, long l) {
        String[] split          = s.split(",");
        //复写currentTimestamp方法，获取当前事件时间
        Long currentTimestamp   =  Long.parseLong(split[1]);
        //对比当前的事件时间和历史最大事件时间，将最新的时间赋值给currentMaxTimestamp变量
        currentMaxTimestamp     = Math.max(currentTimestamp, currentMaxTimestamp);

        return currentMaxTimestamp;
    }
}

/**
 * 通过实现AssignerWithPunctuatedWatermarks接口自定义生成Watermark
 */
class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<String>{
    //复写checkAndGetNextWatermark方法，定义Watermark生成逻辑
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String s, long l) {
        if (s.split(",")[3].equalsIgnoreCase("in"))
            return new Watermark(l);
        else
            return null;
    }
    //复写extractTimestamp方法，定义抽取Timestamp逻辑
    @Override
    public long extractTimestamp(String s, long l) {
        return Long.parseLong(s.split(",")[1]);
    }
}