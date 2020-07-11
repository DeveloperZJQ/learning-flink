package com.stream.watermark;

import com.stream.model.TransInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author happy
 * @create 2020-07-11 20:56
 * @desc 迟到数据不丢弃，另存
 */
public class PeriodWatermarkMain3 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //并行度设置为 1
        env.setParallelism(1);

        OutputTag<TransInfo> lateDataTag = new OutputTag<TransInfo>("late") {
        };

        SingleOutputStreamOperator<TransInfo> data = env.socketTextStream("localhost", 9995)
                .map(new MapFunction<String, TransInfo>() {
                    @Override
                    public TransInfo map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new TransInfo(Long.parseLong(split[0]),split[1],split[2],split[3],split[4]);
                    }
                }).assignTimestampsAndWatermarks(new PeriodWatermark());

        SingleOutputStreamOperator<TransInfo> sum = data.keyBy(0)
                .timeWindow(Time.seconds(10))
//                .allowedLateness(Time.milliseconds(2))
                .sideOutputLateData(lateDataTag)
                .sum(1);

        sum.print();

        sum.getSideOutput(lateDataTag)
                .print();

        env.execute("watermark demo");
    }
}