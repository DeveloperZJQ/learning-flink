package com.stream.streaming.splitflow;

import com.alibaba.fastjson.JSON;
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
    private static final OutputTag<String> aaaTag = new OutputTag<>("aaa"){};
    private static final OutputTag<String> bbbTag = new OutputTag<>("bbb"){};
    private static final OutputTag<String> cccTag = new OutputTag<>("ccc"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MetricEvent> metricEventDataStreamSource = KafkaConfigUtil.buildSource(env);

        SingleOutputStreamOperator<MetricEvent> sideOutputResult = metricEventDataStreamSource.process(new ProcessFunction<MetricEvent, MetricEvent>() {
            @Override
            public void processElement(MetricEvent metricEvent, Context context, Collector<MetricEvent> collector) throws Exception {
                String s = metricEvent.getTags().get("type");
                switch (s) {
                    case "machine":
                        context.output(aaaTag, JSON.toJSONString(metricEvent));
                    case "docker":
                        context.output(bbbTag,JSON.toJSONString(metricEvent));
                    case "application":
                        context.output(cccTag,JSON.toJSONString(metricEvent));
                    default:
                        collector.collect(metricEvent);
                }
            }
        });

        DataStream<String> docker      = sideOutputResult.getSideOutput(aaaTag);
        DataStream<String> application = sideOutputResult.getSideOutput(bbbTag);
        DataStream<String> machine     = sideOutputResult.getSideOutput(cccTag);

        docker.print();
        application.print();
        machine.print();

        env.execute("SideOutput App start");
    }
}
