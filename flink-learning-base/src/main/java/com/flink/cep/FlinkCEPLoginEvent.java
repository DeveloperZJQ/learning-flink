package com.flink.cep;

import com.flink.cep.entity.LoginEvent;
import com.flink.cep.source.LoginSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author : Jhon_yh
 * @author : DeveoperZJQ
 * 模拟检测非法登录请求。3秒钟连续登录两次失败，即输出结果。
 */

public class FlinkCEPLoginEvent {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);

        //添加数据源
        final DataStreamSource<LoginEvent> loginEventDataStreamSource = env.addSource(new LoginSource());

        final SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = loginEventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getLoginTime()));


        final KeyedStream<LoginEvent, Integer> loginEventIntegerKeyedStream = loginEventSingleOutputStreamOperator.keyBy((KeySelector<LoginEvent, Integer>) LoginEvent::getUserId);

        final Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<>() {
            @Override
            public boolean filter(LoginEvent value) {
                return value.getLoginStatus().equals("failed");
            }
        }).next("middle").where(new SimpleCondition<>() {
            @Override
            public boolean filter(LoginEvent value) {
                return value.getLoginStatus().equals("failed");
            }
        }).within(Time.seconds(3));

        final PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventIntegerKeyedStream, pattern);

        final SingleOutputStreamOperator<String> afterPDs = patternStream.process(new PatternProcessFunction<>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) {
                System.out.println(map.toString());
                final LoginEvent start = map.get("start").get(0);
                final LoginEvent middle = map.get("middle").get(0);

                collector.collect(String.format("{%s} login failed, 1st timeStamp: %s, 2nd: %s.", start.getUserName(), start.getLoginTime(), middle.getLoginTime()));
            }
        });
        afterPDs.print("afterPDs: ");
        System.out.println(env.getExecutionPlan());
        env.execute(FlinkCEPLoginEvent.class.getSimpleName());
    }
}