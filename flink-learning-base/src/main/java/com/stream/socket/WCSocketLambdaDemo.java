package com.stream.socket;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @create 2020-07-06 18:46
 * coding by java lambda
 */
public class WCSocketLambdaDemo {
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("please check input params");
            return;
        }
        StreamExecutionEnvironment env                  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        //这段感觉使用了lambda没有省多少代码，体验一般，不如直接上scala
        SingleOutputStreamOperator<Object> sum = stringDataStreamSource.flatMap((s, collector) -> {
            for (String tokens : s.toLowerCase().split("\\W+")) {
                if (tokens.length() > 0) {
                    collector.collect(new Tuple2(tokens, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute("WCSocketLambdaDemo start execute");
    }
}
