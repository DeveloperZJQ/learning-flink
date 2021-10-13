package ch4;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

/**
 * @author happy
 * @since 2021-02-04
 */
public class ForthPointFour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(9000);//每9秒发出一个watermark
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);


        //通过Flink自带的Timestamp Assigner指定的Timestamp和生成的Watermark

        env.execute(ForthPointFour.class.getSimpleName());
    }

    /**
     * 如果用户自定了外部数据源,就不能实现SourceFunction接口来生成流式数据以及相应的EventTime和Watermark,这种情况下就需要借助Timestamp Assigner
     * 来管理数据流中的Timestamp元素和Watermark.Timestamp Assigner 一般是跟在Data Source算子后面指定,也可以在后续的算子中指定,只要保证Timestamp Assigner
     * 在第一个时间相关得Operator之前即可.如果用户已经在SourceFunction中定义Timestamp和Watermarks得生成逻辑,同时又使用了Timestamp Assigner,此时
     * Assigner会覆盖Source Function 中定义的逻辑.
     */

    /**
     * 过滤掉为null和whitespace的字符串
     */
    public static final class FilterClass implements FilterFunction<String> {
        @Override
        public boolean filter(String value) throws Exception {
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * 构造出element以及它的event time.然后把次数赋值为1
     */
    public static final class LineSplitter implements MapFunction<String, Tuple3<String, Long, Integer>> {
        @Override
        public Tuple3<String, Long, Integer> map(String value) throws Exception {
            // TODO Auto-generated method stub
            String[] tokens = value.toLowerCase().split("\\W+");
            long eventtime = Long.parseLong(tokens[1]);
            return new Tuple3<>(tokens[0], eventtime, 1);
        }
    }
}
