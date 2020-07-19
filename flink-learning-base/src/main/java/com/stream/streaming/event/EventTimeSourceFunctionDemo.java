package com.stream.streaming.event;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


/**
 * @author happy
 * @create 2020-07-10 11:15
 * 指定Timestamps与生成Watermarks有两种方式：
 * 1.在Source Function中直接定义Timestamp和Watermarks
 * 2.通过Flink自带的Timestamp Assigner指定Timestamp和生成Watermarks 具体实现见 EventTimeAssignerDemo
 */
public class EventTimeSourceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env   = StreamExecutionEnvironment.getExecutionEnvironment();
        String[] elementInput            = new String[]{
                "110434199209362276,1594360050,,202007071304,in,300",
                "110434199209362276,1594360650,202007071304,in,500",
                "110434199209362276,1594350050,202007071304,out,600",
                "110434199209362276,1594360350,202007071304,out,5",
                "110434199209362276,1594350150,202007071304,in,500"
        };

        DataStreamSource<String> dataStreamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (String s : elementInput) {
                    // 切割每一条数据
                    String[] str = s.split(",");
                    Long timestamp = Long.valueOf(str[1]);
                    // 生成 EventTime 时间戳
                    ctx.collectWithTimestamp(s, timestamp);
                    // 调用 emitWatermark() 方法生成 watermark, 最大延迟设定为 2
                    ctx.emitWatermark(new Watermark(timestamp - 2));
                }
                // 设定默认 watermark
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {
            }
        });

        dataStreamSource.print();

        env.execute("EventTimeSourceFunctionDemo App start");
    }
}
