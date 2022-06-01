package com.flink.datastream;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author DeveloperZJQ
 * @since 2022-5-31
 */
public class DataStreamTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = "127.0.0.1";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> rowData = env.socketTextStream(ip, 9999);

        //map
        SingleOutputStreamOperator<String[]> map = rowData.map(one -> one.split(" "));

        //flatMap
        SingleOutputStreamOperator<String> flatMap = rowData.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            for (String word : s.split(" ")) {
                collector.collect(word);
            }
        });

        //filter
        SingleOutputStreamOperator<String> filter = rowData.filter((FilterFunction<String>) s -> s.length() > 0);

        //keyBy
        KeyedStream<String, String> keyBy1 = rowData.keyBy(one -> one);
        KeyedStream<Tuple2<String, Integer>, String> keyBy2 = rowData.map(row -> Tuple2.of(row, 1)).keyBy(one -> one.f0);

        //reduce
        rowData.map((MapFunction<String, Integer>) Integer::parseInt).keyBy((KeySelector<Integer, Integer>) integer -> integer / 2).reduce((ReduceFunction<Integer>) Integer::sum);

        //window
        WindowedStream<Integer, Integer, TimeWindow> window = rowData.map((MapFunction<String, Integer>) Integer::parseInt)
                .keyBy((KeySelector<Integer, Integer>) integer -> integer / 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //windowAll
        AllWindowedStream<String, TimeWindow> windowAll = rowData.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        //Window Apply
        SingleOutputStreamOperator<Integer> windowApply = window.apply((WindowFunction<Integer, Integer, Integer, TimeWindow>) (integer, window1, iterable, collector) -> {
            int sum = 0;
            sum += integer;
            collector.collect(sum);
        });

        //WindowReduce
        SingleOutputStreamOperator<Integer> windowReduce = window.reduce((ReduceFunction<Integer>) Integer::sum);

        //Union
        DataStream<String> union = flatMap.union(filter);

        //Window Join
        SingleOutputStreamOperator<Integer> map1 = env.socketTextStream(ip, 9998).map((MapFunction<String, Integer>) Integer::parseInt);
        SingleOutputStreamOperator<Integer> map2 = env.socketTextStream(ip, 9997).map((MapFunction<String, Integer>) Integer::parseInt);
        DataStream<Integer> windowJoin = map1.join(map2)
                .where((KeySelector<Integer, Integer>) integer -> integer)
                .equalTo((KeySelector<Integer, Integer>) integer -> integer)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(Integer::sum);

        DataStream<Integer> windowJoinOfSlid = map1.join(map2).where((KeySelector<Integer, Integer>) integer -> integer)
                .equalTo((KeySelector<Integer, Integer>) integer -> integer)
                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(3), Time.seconds(3)))
                .apply(Integer::sum);


        //interval join
        ConnectedStreams<Integer, Integer> connect = map1.connect(map2);
        SingleOutputStreamOperator<Integer> coFlatMap = connect.flatMap(new CoFlatMapFunction<>() {
            @Override
            public void flatMap1(Integer integer, Collector<Integer> collector) {
                collector.collect(integer);
            }

            @Override
            public void flatMap2(Integer integer, Collector<Integer> collector) {
                collector.collect(integer);
            }
        });

        //iterate
        IterativeStream<Integer> iterate = map1.iterate();
        SingleOutputStreamOperator<Integer> feedback = iterate.filter((FilterFunction<Integer>) aLong -> aLong > 10);
        iterate.closeWith(feedback);
        SingleOutputStreamOperator<Integer> iterateOutput = iterate.filter((FilterFunction<Integer>) integer -> integer <= 10);

        //custom partitioning
        DataStream<Integer> partitionCustom = map1.partitionCustom(new MyPartition(), (KeySelector<Integer, Long>) Integer::longValue);

        //Name and Description
        map1.name("1").setDescription("x in (1,2,3,4)");
        env.execute();
    }

    public static class MyPartition implements Partitioner<Long> {
        public MyPartition() {
        }

        @Override
        public int partition(Long key, int numPartitions) {
            if (key % numPartitions == 0) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
