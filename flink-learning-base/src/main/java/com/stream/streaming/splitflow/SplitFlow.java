package com.stream.streaming.splitflow;

import com.happy.common.model.MetricEvent;
import com.happy.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author happy
 * @create 2020-07-19 23:06
 */
public class SplitFlow {
    public static void main(String[] args) throws Exception {
        final ParameterTool params              = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env    = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStreamSource<MetricEvent> data      = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流

        SplitStream<MetricEvent> splitData = data.split(new OutputSelector<MetricEvent>() {
            @Override
            public Iterable<String> select(MetricEvent metricEvent) {
                List<String> tags = new ArrayList<>();
                String s = metricEvent.getTags().get("type");
                switch (s) {
                    case "machine":
                        tags.add("machine");
                        break;
                    case "docker":
                        tags.add("docker");
                    case "application":
                        tags.add("application");
                    default:
                        break;
                }
                return tags;
            }
        });

        DataStream<MetricEvent> machine     = splitData.select("machine");
        DataStream<MetricEvent> docker      = splitData.select("docker");
        DataStream<MetricEvent> application = splitData.select("application");

        machine.print();
        docker.print();
        application.print();

        env.execute("SplitFlow App start");
    }
}
