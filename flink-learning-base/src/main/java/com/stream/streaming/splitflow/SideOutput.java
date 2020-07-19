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
 * @create 2020-07-20 06:09
 */
public class SideOutput {
    private static final OutputTag<MetricEvent> machineTag      =   new OutputTag<MetricEvent>("machine");
    private static final OutputTag<MetricEvent> dockerTag       =   new OutputTag<MetricEvent>("docker");
    private static final OutputTag<MetricEvent> applicationTag  =   new OutputTag<MetricEvent>("application");

    public static void main(String[] args) throws Exception {
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
