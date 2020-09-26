package com.stream.streaming.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @create 2020-07-22 12:43
 * Flink State 深度讲解
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

    //ValueState 使用方式，第一个字段是count，第二个字段是运行的和
    private transient ValueState<Tuple2<Long,Long>> sum;

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long,Long>>() {};
//    }

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        //访问状态的value值
        Tuple2<Long,Long> currentSum    =   sum.value();

        //更新count
        currentSum.f0   +=  1;

        //更新sum
        currentSum.f1   +=  input.f1;

        //更新状态
        sum.update(currentSum);

        //如果count等于2 ，发出平均值并清除状态
        if (currentSum.f0==2){
            out.collect(new Tuple2<>(input.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }


    }
}
