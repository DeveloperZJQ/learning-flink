package com.stream.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @create 2020-07-06 18:12
 */
public class WCSocketDemo {
    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length!=2){
            System.err.println("please check input params.");
            return;
        }
        StreamExecutionEnvironment env                  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        System.out.println("------1111-----");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split("\\W+");
                for (String s1 : splits) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        }).keyBy(0).sum(1);

        sum.print();

        env.execute("okok,go on");
    }
}
