package com.flink.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * {@code WatermarkGenerator} 可以基于事件或者周期性的生成 watermark。
 *
 * <p><b>注意：</b>  WatermarkGenerator 将以前互相独立的 {@code AssignerWithPunctuatedWatermarks}
 * 和 {@code AssignerWithPeriodicWatermarks} 一同包含了进来。
 */
@Public
public interface WatermarkGenerator<T> {

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * 周期性的调用，也许会生成新的 watermark，也许不会。
     */
    void onPeriodicEmit(WatermarkOutput output);
}