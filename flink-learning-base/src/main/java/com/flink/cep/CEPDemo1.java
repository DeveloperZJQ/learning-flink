package com.flink.cep;

import com.flink.cep.entity.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 5s内点击浏览了商品3次
 * 3秒内先点击浏览商品，然后将商品加入收藏
 *
 * @author happy
 * @since 2022/6/15
 * 致敬原作者：https://www.jianshu.com/p/780df5c4e34c
 */
public class CEPDemo1 {

    /**
     * windows开启9999端口 nc -L -p 9999
     * linux开启9999端口  nc -lk 9999
     */
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String ip = "localhost";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, 9999, "\n");

        //过滤掉为null的事件数据
        KeyedStream<Event, String> partitionedInput = dataStreamSource.filter(Objects::nonNull)
                // string 转 Event
                .map(s -> {
                    // 输入的string，逗号分隔，第一个字段为用户名，第二个字段为事件类型
                    String[] strings = s.split(",");
                    if (strings.length != 2) {
                        return null;
                    }
                    //封装事件entity
                    Event event = new Event();
                    event.setName(strings[0]);
                    event.setType(Integer.parseInt(strings[1]));
                    event.setDate(new Date());
                    event.setTimestamp(System.currentTimeMillis());
                    return event;
                }).returns(Event.class)
                // 窗口分配器，指定事件时间戳字段
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()))
                // keyBy之后键值相同的分配到一个分区
                .keyBy(Event::getName);

        partitionedInput.print("partitioned input-->" + partitionedInput);
        // 3秒内先点击浏览商品，然后将商品加入收藏
        Pattern<Event, Event> patternA = Pattern.<Event>begin("firstly")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        // 点击商品
                        return event.getType().equals(0);
                    }
                })
                //目前是松散连续，可以换成next()严格连续
                .followedBy("and")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        // 将商品加入收藏
                        return event.getType().equals(1);
                    }
                }).within(Time.seconds(3));

        // 5s内点击浏览了商品3次。
        Pattern<Event, Event> patternB = Pattern.<Event>begin("start")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        // 浏览商品
                        return event.getType().equals(0);
                    }
                })
                .times(3)
                .within(Time.seconds(5));

        // CEP用pattern将输入的事件流转化为复杂事件流
        PatternStream<Event> patternStreamA = CEP.pattern(partitionedInput, patternA);
        PatternStream<Event> patternStreamB = CEP.pattern(partitionedInput, patternB);

        DataStream<String> streamA = processPatternStream(patternStreamA, "收藏商品");
        DataStream<String> streamB = processPatternStream(patternStreamB, "连续浏览商品3次");

        // 最后两个复杂事件流进行合并
        streamA.union(streamB).print("result-->");
        env.execute(CEPDemo1.class.getSimpleName());
    }

    public static DataStream<String> processPatternStream(PatternStream<Event> patternStream, String tag) {
        return patternStream.process(new PatternProcessFunction<>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                System.out.println("match-->" + match);
                String name = null;
                for (Map.Entry<String, List<Event>> entry : match.entrySet()) {
                    name = entry.getValue().get(0).getName();
                }
                out.collect(name + " 成为潜在客户 ," + tag);
            }
        });
    }

}