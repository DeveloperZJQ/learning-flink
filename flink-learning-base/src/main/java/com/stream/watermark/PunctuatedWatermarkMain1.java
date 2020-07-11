package com.stream.watermark;

import com.stream.model.TransInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @create 2020-07-11 17:19
 */
public class PunctuatedWatermarkMain1 {
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("请输入ip和端口号，如192.168.2.22 9999");
            return;
        }
        StreamExecutionEnvironment env            = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource = env.socketTextStream(args[0], Integer.parseInt(args[1]))
                .setParallelism(1);
        SingleOutputStreamOperator<TransInfo> map = dataStreamSource.map(new MapFunction<String, TransInfo>() {
            @Override
            public TransInfo map(String s) throws Exception {
                String[] split = s.split(",");
                return new TransInfo(Long.parseLong(split[0]), split[1], split[2], split[3], split[4]);
            }
        });

        SingleOutputStreamOperator<TransInfo> transInfoSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(new PunctuatedWatermark());

        transInfoSingleOutputStreamOperator.print();

        env.execute("PunctuatedWatermarkMain1 App start");
    }
}
