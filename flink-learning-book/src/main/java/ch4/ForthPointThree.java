package ch4;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.TimeCharacteristic.*;

/**
 * @author happy
 * @since 2021-02-03
 */
public class ForthPointThree {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);
        System.out.println(getTimeConcept(socketTextStream));
        getStreamTimeCharacteristic(env, "eventTime");

        env.execute(ForthPointThree.class.getSimpleName());
    }

    /**
     * 时间概念类型
     */
    public static String getTimeConcept(DataStreamSource<String> source) {
        return "" +
                "1. 事件生成时间" +
                "2. 事件接入时间" +
                "3. 事件处理时间";
    }

    /**
     * 事件时间,事件接入时间,事件处理时间
     */
    public static void getStreamTimeCharacteristic(StreamExecutionEnvironment env, String e) {
        switch (e) {
            case "eventTime": {
                env.setStreamTimeCharacteristic(EventTime);
                break;
            }
            case "ingestionTime": {
                env.setStreamTimeCharacteristic(IngestionTime);
                break;
            }
            case "processingTime": {
                env.setStreamTimeCharacteristic(ProcessingTime);
                break;
            }
        }
    }
}
