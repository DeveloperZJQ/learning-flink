package com.stream.streaming.splitflow;

import com.happy.common.utils.KafkaConfigUtil;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author happy
 * @create 2020-09-07 12:39
 */
public class SideOutputString {
    //define xml tag and json tag
    private static final OutputTag<String> xmlTag        = new OutputTag<>("xmlData"){};
    private static final OutputTag<String> jsonTag       = new OutputTag<>("jsonData"){};
    public static void main(String[] args) throws Exception {

        //init flink execution env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //data source
        DataStreamSource<String> metricEventDataStreamSource = KafkaConfigUtil.buildSourceString(env);

        SingleOutputStreamOperator<String> process = metricEventDataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                if (s.startsWith("<") && s.endsWith(">")) {
                    context.output(xmlTag, s);
                }
                if (s.startsWith("{") && s.endsWith("}")) {
                    context.output(jsonTag, s);
                }
            }
        });


        DataStream<String> sideOutputXml    = process.getSideOutput(xmlTag);
        DataStream<String> sideOutputJson   = process.getSideOutput(jsonTag);

        sideOutputXml.writeAsText("D:\\tmp\\xml_data", FileSystem.WriteMode.OVERWRITE);
        sideOutputJson.writeAsText("D:\\tmp\\json_data", FileSystem.WriteMode.OVERWRITE);

        env.execute("SideOutputString App start");
    }
}
