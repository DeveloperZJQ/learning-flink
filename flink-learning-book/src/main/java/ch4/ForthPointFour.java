package ch4;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author happy
 * @since 2021-02-04
 */
public class ForthPointFour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);

        //指定Timestamps与生成Watermarks


        env.execute(ForthPointFour.class.getSimpleName());
    }

}
