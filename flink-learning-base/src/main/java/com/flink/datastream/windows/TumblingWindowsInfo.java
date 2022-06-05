package com.flink.datastream.windows;

import com.flink.datastream.entity.Student;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * @author DeveloperZJQ
 * @since 2022-6-1
 */
public class TumblingWindowsInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = "localhost";
        if (args.length > 0) {
            ip = args[0];
        }

        DataStreamSource<String> rowData = env.socketTextStream(ip, 9999);

        SingleOutputStreamOperator<Student> map = rowData
                .map((MapFunction<String, Student>) s -> {
                    String[] words = s.split(" ");
                    if (words.length != 4) {
                        return null;
                    }
                    return new Student(Integer.parseInt(words[0]), words[1], Double.parseDouble(words[2]), System.currentTimeMillis());
                });

        // 指定事件时间戳
        SingleOutputStreamOperator<Student> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getUnixTime()));

        //事件时间滚动窗口
        SingleOutputStreamOperator<Student> evenTimeReduce = watermarks
                .keyBy(Student::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((stu, t1) -> new Student(stu.getId(), stu.getName(), stu.getScore() + t1.getScore(), stu.getUnixTime()));

        //处理时间滚动窗口
        SingleOutputStreamOperator<Student> processTimeReduce = map.keyBy(Student::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((s1, s2) -> new Student(s1.getId(), s1.getName(), s1.getScore() + s2.getScore(), s1.getUnixTime()));

        //事件时间偏移量滚动窗口
        SingleOutputStreamOperator<Student> evenTimeOffset8Reduce = watermarks
                .keyBy(Student::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5), Time.hours(-8)))
                .reduce((stu, t1) -> new Student(stu.getId(), stu.getName(), stu.getScore() + t1.getScore(), stu.getUnixTime()));

        evenTimeReduce.print();
        processTimeReduce.print();
        evenTimeOffset8Reduce.print();
        //process
        env.execute();
    }
}
