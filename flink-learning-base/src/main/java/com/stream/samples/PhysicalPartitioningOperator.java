package com.stream.samples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2020-12-10
 * 五种分区方式
 */
public class PhysicalPartitioningOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9001);
        //随机分区,分区相对均衡，但是比较容易失去原有数据的分区结构
        dataStreamSource.shuffle();
        //通过循环的方式对数据集中的数据进行重分区，能够尽可能保证每个分区的数据均衡，当数据集发生数据倾斜的时候使用这个策略就是比较有效的方法
        dataStreamSource.rebalance();
        //和上面是一种通过循环的方式进行数据重平衡的分区策略。但是不同的是当使用rebalance时，数据会全局性地通过网络介质传输到其他的节点完成数据的重新平衡
        //而rescale仅仅会对上下游继承的算子数据进行重平衡,具体的分区主要根据上下游算子的并行度决定。
        dataStreamSource.rescale();
        //广播策略将输入的数据集复制到下游算子的并行的Tasks实例中，下游算子中的Tasks可以直接从本地内存中获取广播数据集，不再依赖于网络传输。
        //这种策略仅仅适用于数据量小,可以通过广播的方式将小数据集分发到算子的每个分区中。
        dataStreamSource.broadcast();
        //自定义分区器

        env.execute(PhysicalPartitioningOperator.class.getSimpleName());

    }
}
