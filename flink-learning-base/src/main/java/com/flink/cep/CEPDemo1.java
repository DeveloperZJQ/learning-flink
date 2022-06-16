package com.flink.cep;

import com.flink.cep.entity.Event;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author happy
 * @since 2022/6/15
 */
public class CEPDemo1 {

    /**
     * 运行nc -L -p 9999 ，打开端口  win
     * nc -lk 9999     mac
     */
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String ip = "localhost";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, 9999, "\n");

        KeyedStream<Event, String> partitionedInput = dataStreamSource.filter(Objects::nonNull)
                .map(s -> {
                    // 输入的string，逗号分隔，第一个字段为用户名，第二个字段为事件类型
                    String[] strings = s.split(",");
                    if (strings.length != 2) {
                        return null;
                    }
                    Event event = new Event();
                    event.setName(strings[0]);
                    event.setType(Integer.parseInt(strings[1]));
                    return event;
                }).returns(Event.class)
                .keyBy(Event::getName);

        // 先点击浏览商品，然后将商品加入收藏
        Pattern<Event, ?> patternA = Pattern.<Event>begin("firstly")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        // 点击商品
                        return event.getType() == 0;
                    }
                })
                .followedBy("and")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        // 将商品加入收藏
                        return event.getType() == 1;
                    }
                });

        // 1分钟内点击浏览了商品3次。
        Pattern<Event, ?> patternB = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        // 浏览商品
                        return event.getType() == 0;
                    }
                })
                .timesOrMore(3)
                .within(Time.minutes(1));

        // CEP用pattern将输入的时间事件流转化为复杂事件流
        PatternStream<Event> patternStreamA = CEP.pattern(partitionedInput, patternA);
        PatternStream<Event> patternStreamB = CEP.pattern(partitionedInput, patternB);

        DataStream<String> streamA = processPatternStream(patternStreamA, "收藏商品");
        DataStream<String> streamB = processPatternStream(patternStreamB, "连续浏览商品");

        // 最后两个复杂事件流进行合并
        streamA.union(streamB)
                .print();

        env.execute("Flink Streaming Java API Skeleton");
    }

    public static DataStream<String> processPatternStream(PatternStream<Event> patternStream, String tag) {
        return patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
                String name = null;
                for (Map.Entry<String, List<Event>> entry : match.entrySet()) {
                    name = entry.getValue().get(0).getName();
                }
                out.collect(name + " 成为潜在客户 ," + tag);
            }
        });
    }

}