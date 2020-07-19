package com.stream.streaming;

import com.alibaba.fastjson.JSON;
import com.happy.common.model.TransInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author happy
 * @create 2020-07-07 06:24
 * 本段代码主要为体验processing time
 * FLink程序中，默认的时间语义都是processing time
 */
public class ProcessingTimeDemo {
    public static void main(String[] args) throws Exception {

        //判断参数正确传入
        if (args.length!=2){
            System.err.println("please correcting input ip and port");
            return;
        }

        StreamExecutionEnvironment env    = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String ip       =   args[0];
        int port        =   Integer.parseInt(args[1]);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(ip, port);

        //流转换  string->TransInfo
        SingleOutputStreamOperator<TransInfo> map = stringDataStreamSource.map(new MapFunction<String, TransInfo>() {
            @Override
            public TransInfo map(String s) throws Exception {
                TransInfo transInfo = JSON.parseObject(s, TransInfo.class);
                return transInfo;
            }
        });

        // 以卡号分组
        KeyedStream<TransInfo, String> transInfoStringKeyedStream = map.keyBy(new KeySelector<TransInfo, String>() {
            @Override
            public String getKey(TransInfo transInfo) throws Exception {
                return transInfo.getCardId();
            }
        });

        SingleOutputStreamOperator<TransInfo> reduceRes = transInfoStringKeyedStream.timeWindow(Time.seconds(30))
                .reduce(new ReduceFunction<TransInfo>() {
                    TransInfo transInfo2 = new TransInfo();

                    @Override
                    public TransInfo reduce(TransInfo transInfo, TransInfo t1) throws Exception {
                        transInfo2.setUserId(transInfo.getUserId());
                        transInfo2.setCardId(transInfo.getCardId());
                        transInfo2.setTimeStamp(transInfo2.getTimeStamp() + transInfo.getTimeStamp());
                        transInfo2.setFee(transInfo2.getFee() + transInfo.getFee());
                        transInfo2.setTransfer(transInfo2.getTransfer() + transInfo.getTransfer());
                        return transInfo2;
                    }
                });

        reduceRes.print();


        env.execute("ProcessingTimeDemo class test.");
    }
}
