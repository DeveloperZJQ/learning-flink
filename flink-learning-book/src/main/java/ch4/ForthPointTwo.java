package ch4;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2021-02-03
 */
public class ForthPointTwo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);
        printConcept();
        randomPartition(socketTextStream);
        roundrobinPartition(socketTextStream);
        rescalingPartition(socketTextStream);
        broadcast(socketTextStream);
        userDefinePartition(socketTextStream);

        env.execute(ForthPointThree.class.getSimpleName());
    }

    public static void printConcept(){
        String concept = "物理分区(Physical Partitioning)操作的作用是根据指定分区策略将数据重新分区到不同节点的Task案例上执行." +
                "当使用DataStream提供的API对数据处理过程中,依赖于算子本身对数据的分区控制,如果用户希望自己控制数据分区,例如当数据发生了数据倾斜的时候," +
                "就需要通过定义物理分区策略的方式对数据集进行重新分布处理." +
                "flink中已经提供了常见的分区策略,例如随机分区(Random Partitioning)、平衡分区(Roundobin Partitioning)、按比例分区(Roundrobin Partitioning)等" +
                "当然如果给定的分区策略无法满足需求,也可以根据Flink提供的分区控制接口创建分区器,实现自定义分区控制";
        System.out.println(concept);
    }

    /**
     * 随机分区-通过随机的方式将数据分配在下游算子的每个分区中,分区相对均衡,但是较容易失去原有数据的分区结构
     */
    public static DataStream<String> randomPartition(DataStreamSource<String> socketTextStream){
        return socketTextStream.shuffle();
    }

    /**
     * 通过循环的方式对数据集中的数据进行重分区,能够尽可能保证每个分区的数据平衡,当数据集发生数据倾斜的时候使用这种策略就是比较有效的优化方法
     */
    public static DataStream<String> roundrobinPartition(DataStreamSource<String> socketTextStream){
        return socketTextStream.rebalance();
    }

    /**
     * 和Roundrobin Partition一样,Rescaling Partition也是一种通过循环的方式进行数据重平衡的分区策略.但是不同的是,当使用Roundrobin Partition时,
     * 数据会全局性地通过王略介质传输到其他的节点完成数据的重新平衡,而Rescaling Partition仅仅会对上下游继承的算子数据进行重平衡,具体的分区主要根据上下游算子的并行度
     * 决定.
     * 例如上游算子并行度为2,下游算子的并行度为4,就会发生上游算子中的一个分区的数据按照同等比例将数据路由在下游的固定的两个分区中,
     * 另外一个分区同理路由到下游两个分区中.
     */
    public static DataStream<String> rescalingPartition(DataStreamSource<String> socketTextStream){
        return socketTextStream.rescale();
    }

    /**
     * 广播操作-广播策略将输入的数据集复制到下游算子的并行的Tasks实例中,下游算子中的Tasks可以直接从本地内存中获取广播数据集,不再依赖于网络传输.
     * 这种分区策略适合小数据集,例如当大数据集关联小数据集时,可以通过广播的方式将小数据集分布到算子的每个分区中.
     */
    public static DataStream<String> broadcast(DataStreamSource<String> socketTextStream){
        return socketTextStream.broadcast();
    }

    /**
     * 自定义分区(Custom Partitioning) - 除了使用已有的分区器之外,用户也可以实现自定义分区器,然后调用DataStream API上的partitionCustom()方法将创建
     * 的分区器应用到数据集上.
     */
    public static DataStream<String> userDefinePartition(DataStreamSource<String> socketTextStream){
        UserDefinePartitioner userDefinePartitioner = new UserDefinePartitioner();
        return socketTextStream.partitionCustom(userDefinePartitioner, "flink");
    }
}
