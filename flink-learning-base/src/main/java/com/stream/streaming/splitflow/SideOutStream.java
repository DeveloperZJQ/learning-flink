package com.stream.streaming.splitflow;

import com.happy.common.model.MetricEvent;
import com.happy.common.utils.KafkaConfigUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author happy
 * @create 2020-09-03 23:35
 */
public class SideOutStream {
    private static final OutputTag<String> aaaTag = new OutputTag<>("aaa");
    private static final OutputTag<String> bbbTag = new OutputTag<>("bbb");
    private static final OutputTag<String> cccTag = new OutputTag<>("ccc");

    public static void main(String[] args) throws IllegalAccessException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MetricEvent> metricEventDataStreamSource = KafkaConfigUtil.buildSource(env);

        SingleOutputStreamOperator<MetricEvent> sideOutputResult = metricEventDataStreamSource.process(new ProcessFunction<MetricEvent, MetricEvent>() {
            @Override
            public void processElement(MetricEvent metricEvent, Context context, Collector<MetricEvent> collector) throws Exception {
                String s = metricEvent.getTags().get("type");
                switch (s) {
                    case "machine":
                        context.output(machineTag, metricEvent);
                    case "docker":
                        context.output(dockerTag, metricEvent);
                    case "application":
                        context.output(applicationTag, metricEvent);
                    default:
                        collector.collect(metricEvent);
                }
            }
        });

        DataStream<MetricEvent> docker      = sideOutputResult.getSideOutput(dockerTag);
        DataStream<MetricEvent> application = sideOutputResult.getSideOutput(applicationTag);
        DataStream<MetricEvent> machine     = sideOutputResult.getSideOutput(machineTag);

        docker.print();
        application.print();
        machine.print();

        env.execute("SideOutput App start");
    }
}
