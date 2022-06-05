package com.flink.datastream.windows;

import com.flink.datastream.entity.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

/**
 * @author DeveloperZJQ
 * @since 2022-6-5
 */
public class WindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = "127.0.0.1";
        if (args.length != 0) {
            ip = args[0];
        }
        DataStreamSource<String> source = env.socketTextStream(ip, 9999);
        SingleOutputStreamOperator<Student> map = source.map(one -> {
            String[] s = one.split(" ");
            if (s.length != 4) {
                return null;
            }
            return new Student(Integer.parseInt(s[0]), s[1], Double.parseDouble(s[2]), Long.parseLong(s[3]));
        });

        SingleOutputStreamOperator<Student> reduce = map
                .keyBy(Student::getId)
                .window(GlobalWindows.create())
                .reduce((r1, r2) -> new Student(r1.getId(), r1.getName(), r1.getScore() + r2.getScore(), Math.max(r1.getUnixTime(), r2.getUnixTime())));

        reduce.print();
        env.execute();
    }
}
