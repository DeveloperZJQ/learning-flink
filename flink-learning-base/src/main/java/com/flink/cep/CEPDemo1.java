package com.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
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
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //nc -lk 9999
        String ip = "127.0.0.1";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStream<String> datSource = env.socketTextStream(ip, 9999);
        KeyedStream<Event, String> keyedStreamInput = datSource.filter(Objects::nonNull).map(s -> {
            String[] strings = s.split(",");
            if (strings.length != 3) {
                return null;
            }
            return new Event(Integer.parseInt(strings[0]), strings[1], Integer.parseInt(strings[2]));
        }).returns(Event.class)
                .keyBy(Event::getName);

        keyedStreamInput.print("input-->");
        Pattern<Event, Event> pattern1 = Pattern.<Event>begin("firstly")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getType() == 0;
                    }
                })
                .followedBy("and")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getType() == 1;
                    }
                });

        Pattern<Event, Event> pattern2 = Pattern.<Event>begin("start")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getType() == 0;
                    }
                })
                .timesOrMore(1)
                .within(Time.seconds(30));

        PatternStream<Event> patternStream1 = CEP.pattern(keyedStreamInput, pattern1);
        PatternStream<Event> patternStream2 = CEP.pattern(keyedStreamInput, pattern2);

        DataStream<String> streamA = processPatternStream(patternStream1, "收藏商品");
        DataStream<String> streamB = processPatternStream(patternStream2, "连续浏览商品");

        streamA.print("A--");
        streamA.print("B--");
        streamA.union(streamB).print("2222-->");

        env.execute();
    }

    public static DataStream<String> processPatternStream(PatternStream<Event> patternStream, String tag) {
        return patternStream.process(new PatternProcessFunction<>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                String name = null;
                for (Map.Entry<String, List<Event>> entry : match.entrySet()) {
                    name = entry.getValue().get(0).getName();
                    System.out.println("name:"+name);
                }
                out.collect(name + " 成为潜在客户 ," + tag);
            }
        });
    }

    public static class Event {
        private Integer id;
        private String name;
        private Integer type;

        public Event(Integer id, String name, Integer type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getType() {
            return type;
        }

        public void setType(Integer type) {
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(id, event.id) &&
                    Objects.equals(name, event.name) &&
                    Objects.equals(type, event.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, type);
        }

        @Override
        public String toString() {
            return "Event{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", type=" + type +
                    '}';
        }
    }
}
