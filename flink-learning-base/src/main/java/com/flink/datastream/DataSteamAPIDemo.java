package com.flink.datastream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2022/5/23
 */
public class DataSteamAPIDemo {
    public static void main(String[] args) throws Exception {
        //上下文环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取本地文件数据，生成一个DataStream
        DataStreamSource<String> text = env.readTextFile("file:///path/to/file");
        //转换操作
        SingleOutputStreamOperator<Integer> mapParsed = text.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                return Integer.parseInt(value);
            }
        });

        //将结果输出到外部存储介质,地址请自定义
        mapParsed.writeAsText("");

        //触发程序执行
//        env.execute(DataSteamAPIDemo.class.getSimpleName());
        final JobClient jobClient = env.executeAsync();
        JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
        //
    }
}
